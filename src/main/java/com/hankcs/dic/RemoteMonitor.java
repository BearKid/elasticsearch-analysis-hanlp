package com.hankcs.dic;

import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.utility.LexiconUtility;
import com.hankcs.help.ESPluginLoggerFactory;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

/**
 * @project: elasticsearch-analysis-hanlp
 * @description: 自定义远程词典监控线程
 * @author: Kenn
 * @create: 2018-12-14 15:10
 */
public class RemoteMonitor implements Runnable {

    private static final Logger logger = ESPluginLoggerFactory.getLogger(RemoteMonitor.class.getName());

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
     * 数据类型
     */
    private String type;

    private static final String SPLITTER = "\\s";

    public RemoteMonitor(String location, String type) {
        this.location = location;
        this.type = type;
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
            head.setHeader("If-Modified-Since", last_modified);
        }
        if (eTags != null) {
            head.setHeader("If-None-Match", eTags);
        }

        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(head);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                if ((response.getLastHeader("Last-Modified") != null) && !response.getLastHeader("Last-Modified").getValue().equalsIgnoreCase(last_modified)) {
                    loadRemoteCustomWords(response);
                } else if ((response.getLastHeader("ETag") != null) && !response.getLastHeader("ETag").getValue().equalsIgnoreCase(eTags)) {
                    loadRemoteCustomWords(response);
                }
            } else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
                logger.info("remote_ext_dict {} is without modified", location);
            } else {
                logger.info("remote_ext_dict {} return bad code {}", location, response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("remote_ext_dict {} error!", e, location);
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
     *
     * @param response header响应
     */
    private void loadRemoteCustomWords(CloseableHttpResponse response) {
        switch (type) {
            case "custom":
                logger.info("load hanlp remote custom dict path: {}", location);
                loadRemoteWordsUnprivileged(location);
                logger.info("finish load hanlp remote custom dict path: {}", location);
                break;
            case "stop":
                logger.info("load hanlp remote stop words path: {}", location);
                loadRemoteStopWordsUnprivileged(location);
                logger.info("finish load hanlp remote stop words path: {}", location);
                break;
            default:
                return;
        }
        last_modified = response.getLastHeader("Last-Modified") == null ? null : response.getLastHeader("Last-Modified").getValue();
        eTags = response.getLastHeader("ETag") == null ? null : response.getLastHeader("ETag").getValue();
    }

    /**
     * 从远程服务器上下载自定义词条
     *
     * @param location 配置条目
     */
    private void loadRemoteWordsUnprivileged(String location) {
        Tuple<String, Nature> defaultInfo = analysisDefaultInfo(location);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        BufferedReader in = null;
        HttpGet get = new HttpGet(defaultInfo.v1());
        get.setConfig(buildRequestConfig());
        try {
            response = httpclient.execute(get);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                in = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), analysisDefaultCharset(response)));
                String line;
                boolean firstLine = true;
                while ((line = in.readLine()) != null) {
                    if (firstLine) {
                        line = IOUtil.removeUTF8BOM(line);
                        firstLine = false;
                    }

                    final ParsedUpdateCmd parsedUpdateCmd = parseUpdateCmdLine(line);
                    if (parsedUpdateCmd.isAdd()) {
                        CustomDictionary.insert(
                            parsedUpdateCmd.getWord(),
                            analysisNatureWithFrequency(defaultInfo.v2(), parsedUpdateCmd.getNatureAndFrequents())
                        );
                    } else if (parsedUpdateCmd.isDelete()) {
                        CustomDictionary.remove(parsedUpdateCmd.getWord());
                    } else if (parsedUpdateCmd.isSkip()) {
                        // skip, do nothing
                    } else {
                        logger.error(String.format("unknown updateCmd = %s", line));
                    }
                }
                in.close();
                response.close();
            }
            response.close();
        } catch (IllegalStateException | IOException e) {
            logger.error("get remote words {} error", e, location);
        } finally {
            try {
                IOUtils.close(in);
                IOUtils.close(response);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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

    /**
     * 从远程服务器上下载停止词词条
     *
     * @param location 配置条目
     */
    private void loadRemoteStopWordsUnprivileged(String location) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        BufferedReader in = null;
        HttpGet get = new HttpGet(location);
        get.setConfig(buildRequestConfig());
        try {
            response = httpclient.execute(get);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                in = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), analysisDefaultCharset(response)));
                String line;
                boolean firstLine = true;
                while ((line = in.readLine()) != null) {
                    if (firstLine) {
                        line = IOUtil.removeUTF8BOM(line);
                        firstLine = false;
                    }
                    logger.debug("hanlp remote stop word: {}", line);
                    ParsedUpdateCmd updateCmd = parseUpdateCmdLine(line);

                    if(updateCmd.isSkip()){
                        // skip, do nothing
                    } else if(updateCmd.isAdd()) {
                        CoreStopWordDictionary.add(updateCmd.getWord());
                    } else if(updateCmd.isDelete()){
                        CoreStopWordDictionary.remove(updateCmd.getWord());
                    } else {
                        logger.error("unknown update cmd = %s", line);
                    }
                }
                in.close();
                response.close();
            }
            response.close();
        } catch (IllegalStateException | IOException e) {
            logger.error("get remote words {} error", e, location);
        } finally {
            try {
                IOUtils.close(in);
                IOUtils.close(response);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
    private Charset analysisDefaultCharset(CloseableHttpResponse response) {
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
     * @param location 配置路径
     * @return 返回new Tuple<路径, 默认词性>
     */
    private Tuple<String, Nature> analysisDefaultInfo(String location) {
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
}
