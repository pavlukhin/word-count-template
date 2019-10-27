package org.ivan.experiments;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;

public class Hello {
    public static void main(String[] args) {
        try (Ignite client = IgniteStarter.startClient()) {
            client.compute().broadcast(new IgniteRunnable() {
                @Override
                public void run() {
                    System.out.println("Hello Ignite");
                }
            });
        }
    }
}
