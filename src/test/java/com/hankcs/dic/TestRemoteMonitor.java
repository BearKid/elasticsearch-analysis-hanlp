package com.hankcs.dic;

import org.junit.Ignore;
import org.junit.Test;

public class TestRemoteMonitor {

    private String location4MainDic = "http://127.0.0.1:8850/nlp/dictionary/mainWords/forElasticSearchPlugin";
    private String location4StopDic = "http://127.0.0.1:8850/nlp/dictionary/stopWords/forElasticSearchPlugin";
    private String reportFetchStatusURL = "http://127.0.0.1:8850/nlp/dictionary/fetchLog";

    @Ignore
    @Test
    public void test(){
        RemoteMonitor mainWordFetch = new RemoteMonitor(location4MainDic, RemoteMonitor.DicCategory.MAIN, reportFetchStatusURL);
        mainWordFetch.run();
        mainWordFetch.run();

        RemoteMonitor stopWordFetch = new RemoteMonitor(location4StopDic, RemoteMonitor.DicCategory.STOP_WORD, reportFetchStatusURL);
        stopWordFetch.run();
        stopWordFetch.run();
    }
}
