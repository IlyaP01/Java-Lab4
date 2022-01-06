package com.pipeline;

import com.java_polytech.pipeline_interfaces.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;

public class Writer implements IWriter {
    OutputStream output;
    private byte[] buffer;
    private int bufferSize;
    private int bufferIndex = 0;
    private final TYPE[] supportedTypes = { TYPE.BYTE_ARRAY, TYPE.INT_ARRAY, TYPE.CHAR_ARRAY };
    IProvider provider;
    IMediator mediator;
    TYPE intersectType;
    RC rc = RC.RC_SUCCESS;

    @Override
    public RC getRC() {
        return rc;
    }

    @Override
    public RC setOutputStream(OutputStream outputStream) {
        output = outputStream;
        return RC.RC_SUCCESS;
    }

    @Override
    public RC setConfig(String s) {
        ConfigReader configReader = new ConfigReader(RC.RCWho.READER, new WriterConfigGrammar());
        RC rc = configReader.read(s);
        if (!rc.isSuccess())
            return rc;

        if (!configReader.hasKey(WriterConfigGrammar.ConfigParams.BUFFER_SIZE.toStr())) {
            return RC.RC_WRITER_CONFIG_SEMANTIC_ERROR;
        }

        String sizeStr = configReader.getParam(WriterConfigGrammar.ConfigParams.BUFFER_SIZE.toStr());
        try {
            bufferSize = Integer.parseInt(sizeStr);
        }
        catch (NumberFormatException e) {
            return RC.RC_WRITER_CONFIG_SEMANTIC_ERROR;
        }

        if (bufferSize <= 0)
            return RC.RC_WRITER_CONFIG_SEMANTIC_ERROR;

        buffer = new byte[bufferSize];

        return RC.RC_SUCCESS;
    }

    @Override
    public RC setProvider(IProvider iProvider) {
        provider = iProvider;
        boolean isEmptyIntersect = true;
        outerLoop: for (TYPE myType : supportedTypes) {
            for (TYPE providerType : iProvider.getOutputTypes()) {
                if (myType == providerType) {
                    intersectType = myType;
                    isEmptyIntersect = false;
                    break outerLoop;
                }
            }
        }
        if (isEmptyIntersect)
            return RC.RC_EXECUTOR_TYPES_INTERSECTION_EMPTY_ERROR;

        mediator = iProvider.getMediator(intersectType);
        return RC.RC_SUCCESS;
    }

    @Override
    public void run() {
        while (true) {
            byte[] bytesForOutput = null;
            switch (intersectType) {
                case BYTE_ARRAY:
                    bytesForOutput = (byte[]) mediator.getData();
                    break;
                case INT_ARRAY:
                    int[] intArr = (int[]) mediator.getData();
                    if (intArr != null) {
                        ByteBuffer byteBuffer = ByteBuffer.allocate(intArr.length * Integer.BYTES);
                        IntBuffer intBuffer = byteBuffer.asIntBuffer();
                        intBuffer.put(intArr);
                        bytesForOutput = byteBuffer.array();
                    }
                    break;
                case CHAR_ARRAY:
                    char[] charArr = (char[]) mediator.getData();
                    if (charArr != null) {
                        ByteBuffer byteBuffer = ByteBuffer.allocate(charArr.length * Character.BYTES);
                        CharBuffer charBuffer = byteBuffer.asCharBuffer();
                        charBuffer.put(charArr);
                        bytesForOutput = byteBuffer.array();
                    }
                    break;
            }
            if (bytesForOutput == null) {
                if (bufferIndex != 0) {
                    try {
                        output.write(buffer, 0, bufferIndex);
                    } catch (IOException e) {
                        rc = RC.RC_WRITER_FAILED_TO_WRITE;
                        provider.reportError();
                        return;
                    }
                }
                return;
            }
            int i = 0;
            while (i < bytesForOutput.length) {
                buffer[bufferIndex++] = bytesForOutput[i++];
                if (bufferIndex == bufferSize) {
                    try {
                        output.write(buffer);
                    } catch (IOException e) {
                        rc = RC.RC_WRITER_FAILED_TO_WRITE;
                        provider.reportError();
                        return;
                    }
                    bufferIndex = 0;
                }
            }
        }
    }
}
