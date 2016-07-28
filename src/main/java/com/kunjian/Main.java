package com.kunjian;


import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.commons.cli.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by sunkunjian on 2016/7/25.
 */
public class Main {

    private static void dumpMeesage(ByteBufferMessageSet msgs) {
        for (MessageAndOffset msg : msgs) {
            ByteBuffer payload = msg.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            try {
                String mStr = new String(bytes, "utf-8");
                System.out.println(mStr);
            }catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        HashMap<String,Options> commandMap = new HashMap<String, Options>();
        Options fetchTransactionListoptions = new Options();
        Options fetchTransactionMetaOption = new Options();
        Options fetchMessageOption = new Options();
        Option op1 = new Option("z", "zookeeper", true, "zookeeper connect string,xxx.xxx.xxx.xxx:2181");
        op1.setRequired(true);
        fetchTransactionListoptions.addOption(op1);
        fetchTransactionMetaOption.addOption(op1);
        fetchMessageOption.addOption(op1);
        Option op2 = new Option("r", "rootpath", true, "the kafka/transaction root path, /kafka or /transactional");
        op2.setRequired(true);
        fetchTransactionListoptions.addOption(op2);
        fetchTransactionMetaOption.addOption(op2);
        fetchMessageOption.addOption(op2);

        Option tidOpt = new Option("i", "txid", true, "the transaction id string");
        tidOpt.setRequired(true);
        fetchTransactionMetaOption.addOption(tidOpt);

        Option op3 = new Option("t", "topic", true, "the kafka topic name");
        op3.setRequired(true);
        fetchMessageOption.addOption(op3);

        Option op4 = new Option("p", "partition", true, "the partition number");
        op4.setRequired(true);
        fetchMessageOption.addOption(op4);
        fetchTransactionMetaOption.addOption(op4);

        Option op5 = new Option("o", "offset", true, "the offset start from");
        op5.setRequired(true);
        fetchMessageOption.addOption(op5);

        Option op6 = new Option("b", "bytes", true, "the bytes need to fetch");
        op6.setRequired(true);
        fetchMessageOption.addOption(op6);

        commandMap.put("listTx",fetchTransactionListoptions);
        commandMap.put("getTxMeta",fetchTransactionMetaOption);
        commandMap.put("fetchMessage",fetchMessageOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        if(args.length < 1||!(args.length>=1&&commandMap.containsKey(args[0]))) {
            System.out.println("please specify command: listTx, getTxMeta or fetchMessage");
            System.exit(1);
        }
        ArrayList<String > args2 = new ArrayList<String>();
        String inputCmd = args[0];
        Options os = commandMap.get(inputCmd);
        for(int i = 1; i < args.length; i++) {
            args2.add(args[i]);
        }
        CommandLine cmd = null;
        try {
            cmd = parser.parse(os, (args2.toArray(new String[]{})));
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("TridentTool", os);
            System.exit(1);
        }
        if(inputCmd.equals("listTx")) {
            TridentMeta tm = new TridentMeta(cmd.getOptionValue("zookeeper"), cmd.getOptionValue("rootpath"));

            List<String> metaMsg = tm.getTransactions();
            for(String m: metaMsg) {
                System.out.println(m);
            }
            tm.close();
        }
        else if(inputCmd.equals("getTxMeta")) {
            TridentMeta tm = new TridentMeta(cmd.getOptionValue("zookeeper"), cmd.getOptionValue("rootpath"));
            String msg = tm.dumpMeta(cmd.getOptionValue("txid"),Integer.valueOf(cmd.getOptionValue("partition")));
            System.out.println(msg);
            tm.close();
        }
        else if(inputCmd.equals("fetchMessage")) {
            KafkaMessageFetcher km = new KafkaMessageFetcher(cmd.getOptionValue("zookeeper"), cmd.getOptionValue("rootpath"));
            dumpMeesage(km
                    .fetchMessages(cmd.getOptionValue("topic"),
                            Integer.valueOf(cmd.getOptionValue("partition")),
                            Long.valueOf(cmd.getOptionValue("offset")),
                            Integer.valueOf(cmd.getOptionValue("bytes"))));
            km.close();
        }
    }

}
