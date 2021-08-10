/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.greengrass.smbridge.clients;

import software.amazon.awssdk.crt.io.ClientBootstrap;
import software.amazon.awssdk.crt.io.EventLoopGroup;
import software.amazon.awssdk.crt.io.SocketOptions;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnection;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnectionConfig;
import software.amazon.awssdk.eventstreamrpc.GreengrassConnectMessageSupplier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public final class IPCUtils {
    // Port number is not used in domain sockets.
    // It is ignored but the field needs to be set when creating socket connection
    public static final int DEFAULT_PORT_NUMBER = 8033;
    private static EventStreamRPCConnection clientConnection = null;

    private IPCUtils() {

    }

    /**
     * In this package, called by the SMClient class to use with IPC to find custom port for Stream Manager
     * when building a client from the SDK.
     *
     * @return                      an EventStreamRPCConnection representing a connection to a client
     * @throws ExecutionException   thrown if an error in execution
     * @throws InterruptedException thrown if interrupted
     */
    @SuppressWarnings("PMD.CloseResource")
    public static synchronized EventStreamRPCConnection getEventStreamRpcConnection()
            throws ExecutionException, InterruptedException {
        String ipcServerSocketPath = System.getenv("AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT");
        String authToken = System.getenv("SVCUID");
        SocketOptions socketOptions = IPCUtils.getSocketOptionsForIPC();

        if (clientConnection == null) {
            clientConnection = connectToGGCOverEventStreamIPC(socketOptions, authToken, ipcServerSocketPath);
        }
        return clientConnection;
    }

    // removed dependency on kernel, as it is only being used to pull ipcServerSocketPath
    @SuppressWarnings("PMD.NullAssignment")
    private static EventStreamRPCConnection connectToGGCOverEventStreamIPC(SocketOptions socketOptions,
                                                                           String authToken, String ipcServerSocketPath)
            throws ExecutionException, InterruptedException {

        try (EventLoopGroup elGroup = new EventLoopGroup(1);
             ClientBootstrap clientBootstrap = new ClientBootstrap(elGroup, null)) {

            final EventStreamRPCConnectionConfig config =
                    new EventStreamRPCConnectionConfig(clientBootstrap, elGroup, socketOptions, null,
                            ipcServerSocketPath, DEFAULT_PORT_NUMBER,
                            GreengrassConnectMessageSupplier.connectMessageSupplier(authToken));
            final CompletableFuture<Void> connected = new CompletableFuture<>();
            final EventStreamRPCConnection connection = new EventStreamRPCConnection(config);
            final boolean[] disconnected = {false};
            final int[] disconnectedCode = {-1};
            //this is a bit cumbersome but does not prevent a convenience wrapper from exposing a sync
            //connect() or a connect() that returns a CompletableFuture that errors
            //this could be wrapped by utility methods to provide a more
            connection.connect(new EventStreamRPCConnection.LifecycleHandler() {
                //only called on successful connection.
                // That is full on Connect -> ConnectAck(ConnectionAccepted=true)
                @Override
                public void onConnect() {
                    connected.complete(null);
                }

                @Override
                public void onDisconnect(int errorCode) {
                    disconnected[0] = true;
                    disconnectedCode[0] = errorCode;
                    clientConnection = null;
                }

                //This on error is for any errors that is connection level, including problems during connect()
                @Override
                public boolean onError(Throwable t) {
                    connected.completeExceptionally(t);
                    clientConnection = null;
                    return true;    //hints at handler to disconnect due to this error

                }
            });
            connected.get();
            return connection;
        }
    }

    private static SocketOptions getSocketOptionsForIPC() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.connectTimeoutMs = 3000;
        socketOptions.domain = SocketOptions.SocketDomain.LOCAL;
        socketOptions.type = SocketOptions.SocketType.STREAM;
        return socketOptions;
    }
}