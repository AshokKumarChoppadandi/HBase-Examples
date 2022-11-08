package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author
 * Name: Ashok Kumar Choppadandi
 * Created on: 8th Nov 2022
 */

/**
 * Command to execute the program:
 *
 * Syntax:
 * -------
 * java -cp <path to jar> <Main Class> <zookeeper quorum> <zookeeper znode parent>
 *
 * Example:
 * --------
 * java -cp ./HBaseJavaExample-1.0-SNAPSHOT.jar com.bigdata.hbase.CreateHBaseTable zookeeper:2181 /test/hbase
 */

public class CreateHBaseTable {
    public static void main(String[] args) {
        String zookeeperQuorum = args[0];
        String zookeeperParentNode = args[1];
        System.out.println("Zookeeper Quorum :: " + zookeeperQuorum);
        System.out.println("Zookeeper Parent Node :: " + zookeeperParentNode);
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("zookeeper.znode.parent", zookeeperParentNode);

        try {
            System.out.println("Connecting to HBase");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("table1"));
            ColumnFamilyDescriptor descriptor1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1")).build();
            ColumnFamilyDescriptor descriptor2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf2")).build();

            builder.setColumnFamilies(Arrays.asList(descriptor1, descriptor2));

            System.out.println("Creating Table...");
            admin.createTable(builder.build());
            System.out.println("Table created successfully...");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}