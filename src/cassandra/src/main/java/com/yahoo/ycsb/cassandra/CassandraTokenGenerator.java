package com.yahoo.ycsb.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;

public class CassandraTokenGenerator {
    public static void main(String[] args) throws IOException {
        System.out.println("Cassandra token generator");
        int nodes;
        if (args.length >= 1) {
            nodes = new Integer(args[0]);
        } else {
            System.out.println("How many nodes are in your cluster?");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input = reader.readLine();
            nodes = Integer.parseInt(input);
        }
        // token = (i*(2**127))/nodes
        BigInteger base = BigInteger.valueOf(2).pow(127).divide(BigInteger.valueOf(nodes));
        for (int i = 0; i < nodes; i++) {
            BigInteger token = base.multiply(BigInteger.valueOf(i));
            System.out.format("Node %1$d token: %2$s\n", i, token.toString());
        }
    }
}
