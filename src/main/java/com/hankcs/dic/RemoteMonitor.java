package com.hankcs.dic;

import com.google.gson.Gson;
import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.utility.LexiconUtility;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

/**
 * @project: elasticsearch-analysis-hanlp
 * @description: 自定义远程词典监控线程
 * @author: Kenn
 * @create: 2018-12-14 15:10
 */
public class RemoteMonitor implements Runnable {

//    private static final Logger logger = ESPluginLoggerFactory.getLogger(RemoteMonitor.class.getName());
    private static final Logger logger = LoggerFactory.getLogger(RemoteMonitor.class);

    private static CloseableHttpClient httpclient = HttpClients.createDefault();
    /**
     * 上次更改时间
     */
    private String last_modified;
    /**
     * 资源属性
     */
    private String eTags;
    /**
     * 请求地址
     */
    private String location;

    /**
     * 用于上报“拉取更新词典情况”的HTTP URL
     */
    private String reportFetchStatusURL;

    /**
     * 数据类型
     */
    private DicCategory dicCategory;

    private static final String SPLITTER = "\\s";

    public RemoteMonitor(String location, DicCategory dicCategory, String reportFetchStatusURL) {
        this.location = location;
        this.reportFetchStatusURL = reportFetchStatusURL;
        this.dicCategory = dicCategory;
        this.last_modified = null;
        this.eTags = null;
    }

    @Override
    public void run() {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            runUnprivileged();
            return null;
        });
    }

    /**
     * 监控流程：
     * ①向词库服务器发送Head请求
     * ②从响应中获取Last-Modify、ETags字段值，判断是否变化
     * ③如果未变化，休眠1min，返回第①步
     * ④如果有变化，重新加载词典
     * ⑤休眠1min，返回第①步
     */

    private void runUnprivileged() {
        String path = location.split(SPLITTER)[0];

        HttpHead head = new HttpHead(path);
        head.setConfig(buildRequestConfig());

        // 设置请求头
        if (last_modified != null) {
            head.setHeader(HttpHeaders.IF_MODIFIED_SINCE, last_modified);
        }
        if (eTags != null) {
            head.setHeader(HttpHeaders.IF_NONE_MATCH, eTags);
        }

        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(head);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                if (response.getLastHeader(HttpHeaders.LAST_MODIFIED) != null
                    && !response.getLastHeader(HttpHeaders.LAST_MODIFIED).getValue().equalsIgnoreCase(last_modified)
                ) {
                    loadRemoteCustomWords();
                } else if (response.getLastHeader(HttpHeaders.ETAG) != null
                    && !response.getLastHeader(HttpHeaders.ETAG).getValue().equalsIgnoreCase(eTags)
                ) {
                    loadRemoteCustomWords();
                }
            } else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
                logger.info("remote_ext_dict {} is without modified since {}", location, last_modified);
            } else {
                logger.info("remote_ext_dict {} return bad code {}", location, response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            logger.error("remote_ext_dict {} error!", location, e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 加载远程自定义词典
     */
    private void loadRemoteCustomWords() {

        logger.info("load hanlp remote {} dict path: {}", dicCategory, location);
        final Tuple<String, Nature> pathInfo = analyzePath(dicCategory, location);
        final Date lastModifiedOfPreviousFetch = getLastModifiedOfPreviousFetch();
        final DicFetchStatus dicFetchStatus = loadRemoteWordsUnprivileged(
            dicCategory, pathInfo.v1(), lastModifiedOfPreviousFetch, pathInfo.v2()
        );
        logger.info("finish load hanlp remote {} dict path: {}", dicCategory.getType(), location);

        if(dicFetchStatus.getLastModified() != null) {
            last_modified = DateUtils.formatDate(dicFetchStatus.getLastModified());
        }

        if(dicFetchStatus.getETag() != null) {
            eTags = dicFetchStatus.getETag();
        }

        reportToMaster(dicCategory, dicFetchStatus);
    }

    /**
     * 上报某次远程词典拉取任务的结果
     *
     * @param dicCategory
     * @param dicFetchStatus 拉取词典任务的结果
     */
    private void reportToMaster(DicCategory dicCategory, DicFetchStatus dicFetchStatus) {
        if(reportFetchStatusURL != null) {
            final CloseableHttpClient httpClient = HttpClients.createDefault();
            try {
                HttpPost post = new HttpPost(reportFetchStatusURL);
                Map<String, Object> bodyJson = new HashMap<>(4);
                String ip = null;
                try {
                    ip = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    logger.error(e.getMessage(), e);
                }
                bodyJson.put("ip", ip);
                bodyJson.put("dicCategory", dicCategory.getCode());
                bodyJson.put("fetchStart", dicFetchStatus.getFetchStart().getTime());
                bodyJson.put("fetchEnd", dicFetchStatus.getFetchEnd().getTime());
                if(dicFetchStatus.getLastModifiedOfPreviousFetch() != null) {
                    bodyJson.put("lastModifiedOfPreviousFetch", dicFetchStatus.getLastModifiedOfPreviousFetch().getTime());
                }
                if(dicFetchStatus.getLastModified() != null) {
                    bodyJson.put("lastModified", dicFetchStatus.getLastModified().getTime());
                }
                bodyJson.put("successNum", dicFetchStatus.getSuccessNum());
                bodyJson.put("failNum", dicFetchStatus.getFailNum());
                if (dicFetchStatus.getSampleException() != null) {
                    bodyJson.put("sampleExceptionClass", dicFetchStatus.getSampleException().getClass().getName());
                    bodyJson.put("sampleExceptionStack", ExceptionUtils.getStackTrace(dicFetchStatus.getSampleException()));
                }
                post.setEntity(new StringEntity(new Gson().toJson(bodyJson), ContentType.APPLICATION_JSON));
                try {
                    httpClient.execute(post, new BasicResponseHandler());
                } catch (HttpResponseException e) {
                    logger.error("上报拉取远程词典的日志。statusCode = {}", e.getStatusCode(), e);
                } catch (IOException e){
                    logger.error("上报拉取远程词典的日志", e);
                }
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    logger.error("url = " + reportFetchStatusURL, e);
                }
            }
        }
    }

    /**
     * 获取最近一次拉取远程词典时所返回的词典最后修改时间点
     */
    private Date getLastModifiedOfPreviousFetch() {
        if (last_modified == null) {
            return null;
        } else {
            return DateUtils.parseDate(last_modified);
        }
    }

    /**
     * 从远程服务器上下载自定义词条
     *
     * @param dicType 词典类型。停用词词典(stop)或主词典
     * @param httpURL http请求地址
     * @param lastModifiedOfPreviousFetch 最近一次成功拉取远程词典时返回的词典最后修改时间点。
     *                                    告知远程服务器该时间点，服务器应尽量返回自该时间点以后有变动过的词条，而非全量返回。
     *                                    如果为null，则全量返回。
     * @param defaultNature 默认词性。当远程返回的词条不含有词性信息时采用。
     */
    private DicFetchStatus loadRemoteWordsUnprivileged(DicCategory dicType, String httpURL, Date lastModifiedOfPreviousFetch, Nature defaultNature) {
        final DicFetchStatus dicFetchStatus = new DicFetchStatus();
        dicFetchStatus.start();
        dicFetchStatus.setLastModifiedOfPreviousFetch(lastModifiedOfPreviousFetch);
        CloseableHttpClient httpclient = null;
        try {
            httpclient = HttpClients.createDefault();
            HttpGet get = buildHttpGetOfFetchingDic(httpURL, lastModifiedOfPreviousFetch);

            httpclient.execute(get, response -> {
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    Header lastModifiedHeader = response.getLastHeader(HttpHeaders.LAST_MODIFIED);
                    if(lastModifiedHeader != null) dicFetchStatus.setLastModified(DateUtils.parseDate(lastModifiedHeader.getValue()));

                    Header ETagHeader = response.getLastHeader(HttpHeaders.ETAG);
                    if(ETagHeader != null) dicFetchStatus.setETag(ETagHeader.getValue());

                    BufferedReader in = new BufferedReader(new InputStreamReader(
                        response.getEntity().getContent(), analysisDefaultCharset(response))
                    );
                    String line;
                    boolean firstLine = true;
                    while ((line = in.readLine()) != null) {
                        try {
                            if (firstLine) {
                                line = IOUtil.removeUTF8BOM(line);
                                firstLine = false;
                            }

                            final ParsedUpdateCmd updateCmd = parseUpdateCmdLine(line);

                            if(dicType.equals(DicCategory.MAIN)) {
                                if (updateCmd.isAdd()) {
                                    CustomDictionary.insert(
                                        updateCmd.getWord(),
                                        analysisNatureWithFrequency(defaultNature, updateCmd.getNatureAndFrequents())
                                    );
                                    dicFetchStatus.incrSuccessNum();
                                } else if (updateCmd.isDelete()) {
                                    CustomDictionary.remove(updateCmd.getWord());
                                    dicFetchStatus.incrSuccessNum();
                                } else if (updateCmd.isSkip()) {
                                    // skip, do nothing
                                } else {
                                    String msg = String.format("unknown updateCmd = %s", line);
                                    dicFetchStatus.setSampleException(new IllegalArgumentException(msg));
                                    dicFetchStatus.incrFailNum();
                                    logger.error(msg);
                                }
                            } else if(dicType.equals(DicCategory.STOP_WORD)){
                                if(updateCmd.isSkip()){
                                    // skip, do nothing
                                } else if(updateCmd.isAdd()) {
                                    if(!CoreStopWordDictionary.contains(updateCmd.getWord())){
                                        CoreStopWordDictionary.add(updateCmd.getWord());
                                    }
                                    dicFetchStatus.incrSuccessNum();
                                } else if(updateCmd.isDelete()){
                                    if(CoreStopWordDictionary.contains(updateCmd.getWord())) {
                                        CoreStopWordDictionary.remove(updateCmd.getWord());
                                    }
                                    dicFetchStatus.incrSuccessNum();
                                } else {
                                    final String msg = String.format("unknown update cmd = %s", line);
                                    dicFetchStatus.setSampleException(new IllegalArgumentException(msg));
                                    dicFetchStatus.incrFailNum();
                                    logger.error(msg);
                                }
                            }
                        } catch (Exception e) {
                            dicFetchStatus.incrFailNum();
                            dicFetchStatus.setSampleException(e);
                            if (dicFetchStatus.getFailNum() < 3) logger.error(String.format("line = %s", line), e);
                        }
                    }

                } else {
                    dicFetchStatus.setSampleException(
                        new HttpResponseException(
                            response.getStatusLine().getStatusCode(),
                            EntityUtils.toString(response.getEntity(), "utf-8")
                        )
                    );
                }
                return true;
            });

        } catch (Exception e) {
            dicFetchStatus.setSampleException(e);
            Integer statusCode = (e instanceof HttpResponseException) ? ((HttpResponseException)e).getStatusCode() : null;
            logger.error("get remote words {} error, statusCode = {}", httpURL, statusCode, e);
        } finally {
            try {
                if(httpclient != null) httpclient.close();
            } catch (IOException e) {
                logger.error(String.format("fail to close http client, location = %s", httpURL), e);
            }
            dicFetchStatus.end();
        }
        return dicFetchStatus;
    }

    /**
     * 构建用于远程拉取词典的Http Get
     * @param httpURL 远程拉取词典的http地址
     * @param lastModifiedOfPreviousFetch http请求的一个参数，告知上次拉取成功时词典返回的最后修改时间点
     */
    private HttpGet buildHttpGetOfFetchingDic(String httpURL, Date lastModifiedOfPreviousFetch){
        StringBuilder getUrlBuilder = new StringBuilder();
        getUrlBuilder.append(httpURL);
        if(lastModifiedOfPreviousFetch != null){
            if(!httpURL.contains("?")) getUrlBuilder.append("?");
            getUrlBuilder.append("&lastModifiedOfPreviousFetch=");
            final String lastFetchStr = DateUtils.formatDate(lastModifiedOfPreviousFetch);
            try {
                getUrlBuilder.append(URLEncoder.encode(lastFetchStr, "utf-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("encode string = " + lastFetchStr, e);
            }
        }
        HttpGet get = new HttpGet(getUrlBuilder.toString());
        get.setConfig(buildRequestConfig());
        return get;
    }

    /**
     * 将一条词更新命令行解析成 Java Object。
     * 本方法兼容旧版命令行的解析，旧版命令行是没有指定更新操作，默认是add。
     * @param line 命令行, 形如: 某词 [add|delete] 词性A A的频次 词性B B的频次
     */
    private ParsedUpdateCmd parseUpdateCmdLine(String line) {
        String word;
        String updateAction;
        String[] natureAndFreqs;

        // 切分
        String[] param = line.split(SPLITTER);
        word = param[0];

        // 排除空行
        if (word.length() == 0) {
            return new ParsedUpdateCmd(word, ParsedUpdateCmd.UPDATE_ACTION_SKIP, new String[0]);
        } else {

            if (param.length >= 2) {
                if (param[1].equals(ParsedUpdateCmd.UPDATE_ACTION_ADD) || param[1].equals(ParsedUpdateCmd.UPDATE_ACTION_DELETE)) {
                    updateAction = param[1];
                    if (param.length >= 3) {
                        natureAndFreqs = Arrays.copyOfRange(param, 2, param.length);
                    } else {
                        natureAndFreqs = new String[0];
                    }
                } else {
                    updateAction = ParsedUpdateCmd.UPDATE_ACTION_ADD;
                    natureAndFreqs = Arrays.copyOfRange(param, 1, param.length);
                }
            } else {
                updateAction = ParsedUpdateCmd.UPDATE_ACTION_ADD;
                natureAndFreqs = new String[0];
            }
            return new ParsedUpdateCmd(word, updateAction, natureAndFreqs);
        }
    }

    private RequestConfig buildRequestConfig() {
        return RequestConfig.custom()
                .setConnectionRequestTimeout(10 * 1000)
                .setConnectTimeout(10 * 1000)
                .setSocketTimeout(60 * 1000)
                .build();
    }

    /**
     * 分析默认编码
     *
     * @param response 响应
     * @return 返回编码
     */
    private Charset analysisDefaultCharset(HttpResponse response) {
        Charset charset = StandardCharsets.UTF_8;
        // 获取编码，默认为utf-8
        if (response.getEntity().getContentType().getValue().contains("charset=")) {
            String contentType = response.getEntity().getContentType().getValue();
            charset = Charset.forName(contentType.substring(contentType.lastIndexOf("=") + 1));
        }
        return charset;
    }

    /**
     * 解析默认信息
     *
     * @param dicType 词典类型。停用词词典(stop)或一般词典(custom)
     * @param location 配置路径
     * @return 返回new Tuple<路径, 默认词性>
     */
    private Tuple<String, Nature> analyzePath(DicCategory dicType, String location) {
        if(dicType.equals(DicCategory.STOP_WORD)){
            return Tuple.tuple(location, null);
        } else {
            Nature defaultNature = Nature.n;
            String path = location;
            int cut = location.indexOf(' ');
            if (cut > 0) {
                // 有默认词性
                String nature = location.substring(cut + 1);
                path = location.substring(0, cut);
                defaultNature = LexiconUtility.convertStringToNature(nature);
            }
            return Tuple.tuple(path, defaultNature);
        }
    }

    /**
     * 分析词性和频次
     *
     * @param defaultNature 默认词性
     * @param natureWithFreq  由词性、词频作为元素构成的数组。形如[词性A] [A的频次] [词性B] [B的频次]
     * @return 返回字符串，形如[词性A] [A的频次] [词性B] [B的频次] ...
     */
    private String analysisNatureWithFrequency(Nature defaultNature, String[] natureWithFreq) {
        int natureCount = natureWithFreq.length / 2;
        StringBuilder builder = new StringBuilder();
        if (natureCount == 0) {
            builder.append(defaultNature).append(" ").append(1000);
        } else {
            for (int i = 0; i < natureCount; ++i) {
                Nature nature = LexiconUtility.convertStringToNature(natureWithFreq[2 * i]);
                int frequency = Integer.parseInt(natureWithFreq[1 + 2 * i]);
                builder.append(nature).append(" ").append(frequency);
                if (i != natureCount - 1) {
                    builder.append(" ");
                }
            }
        }
        return builder.toString();
    }

    /**
     * 词典拉取更新时词典的一行
     */
    private class ParsedUpdateCmd {
        public static final String UPDATE_ACTION_SKIP = "skip";
        public static final String UPDATE_ACTION_ADD = "add";
        public static final String UPDATE_ACTION_DELETE = "delete";

        private String word;
        private String updateAction;
        private String[] natureAndFrequents;

        public ParsedUpdateCmd(String word, String updateAction, String[] natureAndFrequents) {
            this.word = word;
            this.updateAction = updateAction;
            this.natureAndFrequents = natureAndFrequents;
        }

        public String getWord() {
            return word;
        }

        public String getUpdateAction() {
            return updateAction;
        }

        public String[] getNatureAndFrequents() {
            return natureAndFrequents;
        }

        public boolean isSkip(){
            return updateAction.equals(UPDATE_ACTION_SKIP);
        }

        public boolean isAdd(){
            return updateAction.equals(UPDATE_ACTION_ADD);
        }

        public boolean isDelete(){
            return updateAction.equals(UPDATE_ACTION_DELETE);
        }

        @Override
        public String toString() {
            return "ParsedUpdateCmd{" +
                "word='" + word + '\'' +
                ", updateAction='" + updateAction + '\'' +
                ", natureAndFrequents=" + Arrays.toString(natureAndFrequents) +
                '}';
        }
    }

    /**
     * 词典拉取更新的结果
     */
    private class DicFetchStatus {
        /**
         * 开始拉取的时间点
         */
        private Date fetchStart;

        /**
         * 结束拉取的时间点
         */
        private Date fetchEnd;

        /**
         * 拉取更新成功的词条数
         */
        private int successNum;

        /**
         * 拉取更新失败的词条数
         */
        private int failNum;

        /**
         * 拉取更新过程中发生的异常的其中一个异常示例
         */
        private Throwable exceptionSample;

        /**
         * 上一次拉取远程词典时，返回的词典最近修改时间点
         */
        private Date lastModifiedOfPreviousFetch;

        /**
         * 本次拉取远程词典时，返回的词典最近修改的时间点
         */
        private Date lastModified;

        /**
         * 本次拉取远程词典时，HTTP响应头的ETag值
         */
        private String ETag;

        public void start(){
            if(fetchStart != null) throw new IllegalStateException(
                String.format("this fetch was already start at %s", fetchStart.toString())
            );
            fetchStart = new Date();
        }

        public void end(){
            if(fetchEnd != null) throw new IllegalStateException(
                String.format("this fetch was already end at %s", fetchEnd.toString())
            );
            fetchEnd = new Date();
        }

        public Date getFetchStart() {
            return fetchStart;
        }

        public Date getFetchEnd() {
            return fetchEnd;
        }

        public int getTotalNum() {
            return successNum + failNum;
        }

        public int getSuccessNum() {
            return successNum;
        }

        public int getFailNum() {
            return failNum;
        }

        public Throwable getSampleException() {
            return exceptionSample;
        }

        public void incrSuccessNum(){
            successNum += 1;
        }

        public void incrFailNum(){
            failNum += 1;
        }

        public void setSampleException(Throwable e){
            exceptionSample = e;
        }

        public Date getLastModifiedOfPreviousFetch() {
            return lastModifiedOfPreviousFetch;
        }

        public void setLastModifiedOfPreviousFetch(Date lastModifiedOfPreviousFetch) {
            this.lastModifiedOfPreviousFetch = lastModifiedOfPreviousFetch;
        }

        public Date getLastModified() {
            return lastModified;
        }

        public void setLastModified(Date lastModified) {
            this.lastModified = lastModified;
        }

        public String getETag() {
            return ETag;
        }

        public void setETag(String ETag) {
            this.ETag = ETag;
        }
    }

    /**
     * 拉取的词典类型
     */
    public static class DicCategory{
        /**
         * 词典类型 - 主词典
         */
        public static final DicCategory MAIN = new DicCategory("custom", 0);

        /**
         * 词典类型 - 停用词典
         */
        public static final DicCategory STOP_WORD = new DicCategory("stop", 1);

        private String type;

        private int code;

        public DicCategory(String type, int code) {
            this.type = type;
            this.code = code;
        }

        public String getType() {
            return type;
        }

        public int getCode() {
            return code;
        }

        public static DicCategory fromType(String type){
            if(type.equals(MAIN.getType())) return MAIN;
            else if(type.equals(STOP_WORD.getType())) return STOP_WORD;
            else throw new IllegalArgumentException(String.format("未知type(%s)", type));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DicCategory that = (DicCategory) o;
            return code == that.code &&
                Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, code);
        }

        @Override
        public String toString() {
            return "DicCategory{" +
                "type='" + type + '\'' +
                ", code=" + code +
                '}';
        }
    }

}
