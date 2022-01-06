package com.pipeline;

import com.java_polytech.pipeline_interfaces.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

public class Reader implements IReader {
    InputStream input;
    private byte[] buffer;
    private int bufferSize;
    private ArrayBlockingQueue<byte[]> queue; //queue of packages with data
    final private TYPE[] supportedTypes = { TYPE.BYTE_ARRAY, TYPE.CHAR_ARRAY, TYPE.INT_ARRAY };
    private RC rc = RC.RC_SUCCESS;
    private boolean isErrInConsumer = false;

    @Override
    public RC getRC() {
        return rc;
    }

    @Override
    public void reportError() {
        isErrInConsumer = true;
    }

    @Override
    public RC setInputStream(InputStream inputStream) {
        input = inputStream;
        return RC.RC_SUCCESS;
    }

    @Override
    public void run() {
        int sizeOfData;
        try {
            sizeOfData = input.read(buffer, 0, bufferSize);
        } catch (IOException e) {
            rc = RC.RC_READER_FAILED_TO_READ;
            try {
                queue.put(new byte[0]);
            } catch (InterruptedException ex) {
                System.out.println("The second exception while handling the first exception - can not handle!");
            }
            return;
        }
        while (sizeOfData > 0 && !isErrInConsumer) {
            try {
                queue.put(Arrays.copyOf(buffer, sizeOfData));
                sizeOfData = input.read(buffer, 0, bufferSize);
            } catch (IOException e) {
                rc = RC.RC_READER_FAILED_TO_READ;
                try {
                    queue.put(new byte[0]);
                } catch (InterruptedException ex) {
                    System.out.println("The second exception while handling the first exception - can not handle!");
                }
                return;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                rc = new RC(RC.RCWho.READER, RC.RCType.CODE_CUSTOM_ERROR, "Queue exception");
            }
        }

        try {
            queue.put(new byte[0]);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            rc = new RC(RC.RCWho.READER, RC.RCType.CODE_CUSTOM_ERROR, "Queue exception");
        }
    }

    @Override
    public RC setConfig(String s) {
        ConfigReader configReader = new ConfigReader(RC.RCWho.READER, new ReaderConfigGrammar());
        RC rc = configReader.read(s);
        if (!rc.isSuccess())
            return rc;

        if (!configReader.hasKey(ReaderConfigGrammar.ConfigParams.BUFFER_SIZE.toStr())) {
            return RC.RC_READER_CONFIG_SEMANTIC_ERROR;
        }

        if (!configReader.hasKey(ReaderConfigGrammar.ConfigParams.QUEUE_SIZE.toStr())) {
            return RC.RC_READER_CONFIG_SEMANTIC_ERROR;
        }

        String sizeStr = configReader.getParam(ReaderConfigGrammar.ConfigParams.BUFFER_SIZE.toStr());
        try {
            bufferSize = Integer.parseInt(sizeStr);
        }
        catch (NumberFormatException e) {
            return RC.RC_READER_CONFIG_SEMANTIC_ERROR;
        }

        if (bufferSize <= 0)
            return RC.RC_READER_CONFIG_SEMANTIC_ERROR;

        String queueSizeStr= configReader.getParam(ReaderConfigGrammar.ConfigParams.QUEUE_SIZE.toStr());
        try {
            int queueSize = Integer.parseInt(queueSizeStr);
            queue = new ArrayBlockingQueue<>(queueSize);
        }
        catch (NumberFormatException e) {
            return RC.RC_READER_CONFIG_SEMANTIC_ERROR;
        }

        buffer = new byte[bufferSize];

        return RC.RC_SUCCESS;
    }

    @Override
    public TYPE[] getOutputTypes() {
        return supportedTypes;
    }

    @Override
    public IMediator getMediator(TYPE type) {
        if (type == TYPE.BYTE_ARRAY) {
            return () -> {
                try {
                    byte[] out = queue.take();
                    if (out.length > 0)
                        return out;
                    else {
                        queue.put(new byte[0]);
                        return null;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    rc = new RC(RC.RCWho.READER, RC.RCType.CODE_CUSTOM_ERROR, "Queue exception");
                    return null;
                }
            };
        }
        else if (type == TYPE.CHAR_ARRAY) {
            return () -> {
                assert (bufferSize % 2 == 0);
                try {
                    byte[] out = queue.take();
                    if (out.length == 0) {
                        queue.put(new byte[0]);
                        return null;
                    }
                    ByteBuffer b = ByteBuffer.wrap(out);
                    return b.asCharBuffer().array();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    rc = new RC(RC.RCWho.READER, RC.RCType.CODE_CUSTOM_ERROR, "Queue exception");
                   return null;
                }
            };
        }
        else if (type == TYPE.INT_ARRAY) {
            return () -> {
                assert(bufferSize % 4 == 0);
                try {
                    byte[] out = queue.take();
                    if (out.length == 0) {
                        queue.put(new byte[0]);
                        return null;
                    }
                    ByteBuffer b = ByteBuffer.wrap(out);
                    return b.asIntBuffer().array();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    rc = new RC(RC.RCWho.READER, RC.RCType.CODE_CUSTOM_ERROR, "Queue exception");
                    return null;
                }
            };
        }
        else
            return null;
    }
}
