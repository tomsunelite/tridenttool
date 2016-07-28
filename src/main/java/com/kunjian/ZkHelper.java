package com.kunjian;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.List;

/**
 * Created by sunkunjian on 2016/7/27.
 */
public class ZkHelper {
    private static ZkHelper zkHelper = null;
    private static CuratorFramework _curator = null;

    private ZkHelper() {
        super();
    }
    public static ZkHelper getInstance(String zkStr) {
        if(zkHelper == null) {
            synchronized (ZkHelper.class) {
                if (zkHelper == null) {
                    try {
                        _curator = CuratorFrameworkFactory.newClient(
                                zkStr,
                                20000,
                                15000,
                                new RetryNTimes(5, 1000));
                        _curator.start();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
            zkHelper = new ZkHelper();
        }
        return zkHelper;
    }

    public byte[] getData(String path) {
        try {
            return _curator.getData().forPath(path);
        } catch (RuntimeException e) {
             e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] getData(String path, Boolean doNotPrintEx) {
        try {
            return _curator.getData().forPath(path);
        }catch (Exception e) {
            if(!doNotPrintEx) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public List<String> getChildrens(String path) {
        try {
            return _curator.getChildren().forPath(path);
        }catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public void releaseConnection() {
        if(_curator != null) {
            _curator.close();
            _curator = null;
        }
    }
}
