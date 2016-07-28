package com.kunjian;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by sunkunjian on 2016/7/27.
 */
public class TridentMeta {
    private String _rootPath = ""; // kafka zk path
    private ZkHelper _zkHelper = null;
    public TridentMeta(String zkString,String rootPath) {
        _rootPath = rootPath;
        _zkHelper = ZkHelper.getInstance(zkString);
    }

    public String dumpMeta(String tx, Integer partition) {
        String txId = getCurTxId(tx);
        String msg = "";
        String path = _rootPath + "/" + tx + "/user/partition_" + partition + "/" + txId;
        byte[] data = _zkHelper.getData(path,true);
        Integer maxRetry = 10000;
        while (data == null && maxRetry>0) {
            txId = getCurTxId(tx);
            path = _rootPath + "/" + tx + "/user/partition_" + partition + "/" + txId;
            data = _zkHelper.getData(path,true);
            try {
                Thread.sleep(100);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            maxRetry--;
        }
        try {
            if(data != null) {
                msg = new String(data, "UTF-8");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return msg;
    }



    private String getCurTxId(String tx){
        byte[] idByte = _zkHelper.getData(_rootPath+"/"+tx+"/coordinator/currtx");
        try {
            return new String(idByte, "UTF-8");
        }catch (Exception ex) {
            ex.printStackTrace();
        }
        return "";
    }
    private HashMap<String,String> getCurTxId() {
        HashMap<String, String> curTxIdMap = new HashMap<String, String>();
        List<String> txs = getTransactions();
        for(String tx: txs) {
            byte[] idByte = _zkHelper.getData(_rootPath+"/"+tx+"/coordinator/currtx");
            try {
                curTxIdMap.put(tx, new String(idByte, "UTF-8"));
            }catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return curTxIdMap;
    }

    public List<String> getTransactions() {
        List<String> children = new LinkedList<String>();
        try {
            children = _zkHelper.getChildrens(_rootPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return children;
    }

    public void close() {
        _zkHelper.releaseConnection();
    }
}
