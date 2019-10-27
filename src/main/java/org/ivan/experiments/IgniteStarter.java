package org.ivan.experiments;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;

public class IgniteStarter {
    public static void main(String[] args) {
        startServer();
    }

    public static Ignite startServer() {
        return start(false);
    }

    public static Ignite startClient() {
        return start(true);
    }

    private static Ignite start(boolean client) {
        return Ignition.start(new IgniteConfiguration()
                .setClientMode(client)
                .setPeerClassLoadingEnabled(true)
                .setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(new TcpDiscoveryVmIpFinder()
                                .setAddresses(Collections.singleton("localhost:47500..47509")))));
    }
}
