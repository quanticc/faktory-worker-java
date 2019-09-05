package com.github.quanticc.faktory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;

public class FaktoryConnection {

    private static final Logger log = LoggerFactory.getLogger(FaktoryConnection.class);

    private final URI uri;

    private Socket socket;
    private BufferedReader fromServer;
    private DataOutputStream toServer;

    public FaktoryConnection(URI uri) {
        this.uri = uri;
    }

    boolean isConnected() {
        return socket != null && socket.isConnected();
    }

    String handshake() throws IOException, FaktoryConnectionError {
        socket = openSocket();
        fromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        toServer = new DataOutputStream(socket.getOutputStream());

        String response = readFromSocket();

        if (response == null || !response.startsWith("+HI")) {
            throw new FaktoryConnectionError(response);
        }

        log.debug("> {}", response);
        return response.split(" ", 2)[1];
    }

    void connect(String helloPayload) throws IOException, FaktoryConnectionError {
        send("HELLO " + helloPayload);
    }

    String send(String message) throws FaktoryConnectionError, IOException {
        writeToSocket(message);
        String response = readFromSocket();
        if (response != null && response.startsWith("-")) {
            throw new FaktoryConnectionError(response);
        }
        return response;
    }

    private Socket openSocket() throws IOException {
        return socket != null ? socket : new Socket(uri.getHost(), uri.getPort());
    }

    private String readFromSocket() throws IOException {
        int bufferSize = 4096;
        char[] buf = new char[bufferSize];
        int read = fromServer.read(buf);
        log.trace("Bytes read: {}", read);
        String buffer = new String(buf).replace("\u0000", "");
        boolean buffering;
        while (true) {
            buffering = true;
            while (buffering) {
                if (buffer.contains("\r\n")) {
                    String[] tokens = buffer.split("\r\n", 2);
                    String line = tokens[0];
                    buffer = tokens[1];
                    log.trace("Split: \"{}\", \"{}\"", line, buffer);
                    if (line.isEmpty()) {
                        continue;
                    } else if (line.charAt(0) == '+') {
                        String reply = line.trim();
                        log.debug("> {}", reply);
                        return reply;
                    } else if (line.charAt(0) == '-') {
                        String reply = line.trim();
                        log.debug("> {}", reply);
                        return reply;
                    } else if (line.charAt(0) == '$') {
                        // read $xxx bytes of data into a buffer
                        int toRead = Integer.parseInt(line.substring(1)) + 2;
                        log.trace("Bytes to read: {}", toRead);
                        if (toRead <= 1) {
                            log.info("> nil");
                            return null;
                        } else {
                            String data;
                            if (buffer.length() >= toRead) {
                                log.trace("Enough bytes in the buffer: {}", buffer.length());
                                // we've already got enough bytes in the buffer
                                data = buffer.substring(0, toRead);
                                buffer = buffer.substring(toRead);
                            } else {
                                data = buffer;
                                while (data.length() != toRead) {
                                    int required = toRead - data.length();
                                    char[] bb = new char[required];
                                    int bulkRead = fromServer.read(bb);
                                    log.trace("Read {} bytes while in bulk", bulkRead);
                                    data += new String(bb);
                                }
                                buffer = "";
                            }
                            String reply = data.trim();
                            log.debug("> {}", reply);
                            return reply;
                        }
                    }
                } else {
                    char[] bb = new char[bufferSize];
                    int moreRead = fromServer.read(bb);
                    log.trace("More bytes read: {}", moreRead);
                    String more = new String(bb);
                    if (more.isEmpty()) {
                        buffering = false;
                    } else {
                        buffer += more;
                    }
                }
            }
        }
    }

    private void writeToSocket(String content) throws IOException {
        log.debug("{}", content);
        toServer.writeBytes(content + "\n");
    }

    void close() throws IOException {
        socket.close();
    }
}
