package com.pipeline;

import com.java_polytech.pipeline_interfaces.*;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class RLEExecutor implements IExecutor {
    final private TYPE[] supportedTypes = { TYPE.BYTE_ARRAY };

    private enum Mode {
        ENCODE ("ENCODE"),
        DECODE ("DECODE");
        private final String str;

        Mode(String str) {
            this.str = str;
        }
        String toStr() {
            return str;
        }
    }

    // Config
    private Mode mode;
    private int bufferSize;
    private IProvider provider;
    private IMediator mediator;
    private final ArrayList<Thread> executorsThreads = new ArrayList<>();
    private int numOfThreads;
    private long maxPackagesNum;

    // Error in this executor or in consumer
    private RC rc = RC.RC_SUCCESS;
    private boolean isConsumerError;

    final private ConcurrentHashMap<Long, byte[]> packages = new ConcurrentHashMap<>();

    // to save right order of packages
    private long inputPackageNumber = 0;
    private long outPackageNumber = 0;

    private interface ICoder {
        RC run(byte[] bytes);
    }

    private class RLEExecutorRunnable implements Runnable {
        private final ByteArrayOutputStream buffer;
        private final ICoder coder;
        private final IMediator mediator;

        RLEExecutorRunnable(int bufferSize, Mode mode, IMediator mediator) {
            buffer = new ByteArrayOutputStream(bufferSize);
            coder = mode == Mode.ENCODE ? new Encoder() : new Decoder();
            this.mediator = mediator;
        }

        private class Encoder implements ICoder {
            private byte prevByte = 0;
            private byte repeatingByte = 0;
            private byte counter = 0;
            private int startOfSingles = 0;

            // helper method
            // set counter, repeatingByte and startOfSingles values when new sequence of
            // the same or different symbols starts
            private int startWith(int i, byte[] bytes) {
                if (i >= bytes.length) {
                    counter = 0;
                    return i;
                }

                if (i + 1 == bytes.length) {
                    counter = 1;
                    repeatingByte = bytes[i];
                    return i + 1;
                }

                if (bytes[i + 1] == bytes[i]) {
                    counter = 2;
                    repeatingByte = bytes[i + 1];
                }
                else {
                    counter = -2;
                    startOfSingles = i;
                }

                prevByte = bytes[i + 1];

                return i + 2;
            }

            public RC run(byte[] bytes) {
                if (bytes == null)
                    return RC.RC_SUCCESS;
                int i = startWith(0, bytes);
                for (; i < bytes.length; ++i) {
                    if (counter == Byte.MAX_VALUE) {
                        buffer.write(counter);
                        buffer.write(repeatingByte);
                        i = startWith(i, bytes);
                    }
                    if (counter == Byte.MIN_VALUE) {
                        buffer.write(counter);
                        buffer.write(bytes, startOfSingles, -counter);
                        i = startWith(i, bytes);
                    }

                    if (i >= bytes.length)
                        break;

                    if (bytes[i] == prevByte) {
                        if (counter > 0)
                            ++counter;
                        else {
                            buffer.write((byte) (counter + 1));
                            buffer.write(bytes, startOfSingles, -counter - 1);
                            counter = 2;
                            repeatingByte = bytes[i];
                        }
                    }
                    else {
                        if (counter > 0) {
                            buffer.write(counter);
                            buffer.write(repeatingByte);
                            i = startWith(i, bytes) - 1;
                        }
                        else {
                            --counter;
                        }
                    }
                    prevByte = bytes[i];
                }

                if (counter > 0) {
                    buffer.write(counter);
                    buffer.write(repeatingByte);
                }
                else if (counter < 0) {
                    buffer.write(counter);
                    buffer.write(bytes, startOfSingles, -counter);
                }

                return RC.RC_SUCCESS;
            }
        }

        private class Decoder implements ICoder {
            // if the sequence of different symbols to decode is taller than current input buffer
            private int restToDecode = 0;
            // if there is a count of the end of input buffer and encoded symbol if next buffer
            private int prevCount = 0;

            @Override
            public RC run(byte[] bytes) {
            /* if algorithm was written correctly then we must
           write the rest of different symbols from previous buffer
           or the first symbol prevCount times, but not at the same time */
                assert(prevCount >= 0 && restToDecode == 0 ||
                        prevCount == 0 && restToDecode >= 0);

                if (bytes == null) {
                    if (restToDecode != 0 || prevCount != 0)
                        return new RC(RC.RCWho.EXECUTOR, RC.RCType.CODE_CUSTOM_ERROR, "Invalid RLE code or decoder use more than one thread!");
                    return RC.RC_SUCCESS;
                }
                int i = 0;
                if (prevCount > 0) {
                    for (int j = 0; j < prevCount; ++j) {
                        buffer.write(bytes[0]);
                    }
                    ++i;
                    prevCount = 0;
                }
                else if (restToDecode > 0) {
                    int restInThisBuffer = Integer.min(bytes.length, restToDecode);
                    buffer.write(bytes, 0, restInThisBuffer);
                    i += restInThisBuffer;
                    restToDecode -= restInThisBuffer;
                }

                while (i < bytes.length)  {
                    int count = bytes[i];
                    if (i + 1 == bytes.length) {
                        restToDecode = count > 0 ? 0 : -count;
                        prevCount =  Integer.max(count, 0);
                        return RC.RC_SUCCESS;
                    }
                    if (count > 0) {
                        byte sym =  bytes[i + 1];
                        for (int j = 0; j < count; ++j) {
                            buffer.write(sym);
                        }
                        i += 2;
                    }
                    else {
                        count = -count;
                        ++i;
                        if (count > bytes.length - i) {
                            restToDecode = count - (bytes.length - i);
                            count = bytes.length - i;
                        }
                        buffer.write(bytes, i, count);
                        i += count;
                    }
                }

                return RC.RC_SUCCESS;
            }
        }

        @Override
        public void run() {
            byte[] data;
            long thisPackageNumber;
            while (true) {
                synchronized (provider) { // all executors have the same provider
                    // Executors need to save the order of packages, so they enumerate all input packages
                    // Certainly, it must be an atomic operation (all executors must have different numbers)
                    data = (byte[]) mediator.getData();
                    thisPackageNumber = inputPackageNumber;
                    // We don't need to process overflows because rang of long always is mush more than
                    // current number of processing packages
                    ++inputPackageNumber;
                }

                rc = coder.run(data);
                if (!rc.isSuccess() || isConsumerError) {
                    provider.reportError();
                    packages.put(thisPackageNumber, new byte[0]);
                    return;
                }

                // When file is very big and writer works slowly (e.g. because of small buffer)
                // executor can accumulate too many packages, and it causes out of memory
                while (packages.size() > maxPackagesNum) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                packages.put(thisPackageNumber, buffer.toByteArray());

                if (data == null) {
                    packages.put(++inputPackageNumber, new byte[0]);
                    return;
                }

                buffer.reset();
            }
        }
    }

    @Override
    public RC setConfig(String s) {
        ConfigReader configReader = new ConfigReader(RC.RCWho.EXECUTOR, new ExecutorConfigGrammar());
        RC rc = configReader.read(s);
        if (!rc.isSuccess())
            return rc;

        if (!configReader.hasKey(ExecutorConfigGrammar.ConfigParams.BUFFER_SIZE.toStr())) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }
        String sizeStr = configReader.getParam(ExecutorConfigGrammar.ConfigParams.BUFFER_SIZE.toStr());
        try {
            bufferSize = Integer.parseInt(sizeStr);
        }
        catch (NumberFormatException e) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }
        if (bufferSize <= 0)
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;

        if (!configReader.hasKey(ExecutorConfigGrammar.ConfigParams.MODE.toStr())) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }
        String modeStr = configReader.getParam(ExecutorConfigGrammar.ConfigParams.MODE.toStr());
        if (modeStr.equalsIgnoreCase(Mode.ENCODE.toStr()))
            mode = Mode.ENCODE;
        else if (modeStr.equalsIgnoreCase(Mode.DECODE.toStr()))
            mode = Mode.DECODE;
        else
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;

        if (!configReader.hasKey(ExecutorConfigGrammar.ConfigParams.NUM_OF_THREADS.toStr())) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }
        String numOfThreadsStr = configReader.getParam(ExecutorConfigGrammar.ConfigParams.NUM_OF_THREADS.toStr());
        try {
            numOfThreads = Integer.parseInt(numOfThreadsStr);
        } catch (NumberFormatException e) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }
        if (numOfThreads < 1)
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;

        if (numOfThreads > 1 && mode == Mode.DECODE) {
            // It's not always an error. We didn't specify warnings, so I just print it to console.
            System.out.println("Warning: if decoder is not next after encoder, it must work in one thread to avoid errors!");
        }

        if (!configReader.hasKey(ExecutorConfigGrammar.ConfigParams.MAX_PACKAGES_NUM.toStr())) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }
        String maxPackagesNumStr = configReader.getParam(ExecutorConfigGrammar.ConfigParams.MAX_PACKAGES_NUM.toStr());
        try {
            maxPackagesNum = Integer.parseInt(maxPackagesNumStr);
        } catch (NumberFormatException e) {
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;
        }

        if (maxPackagesNum < 1)
            return RC.RC_EXECUTOR_CONFIG_SEMANTIC_ERROR;

        return RC.RC_SUCCESS;
    }

    @Override
    public RC setProvider(IProvider iProvider) {
        provider = iProvider;
        boolean isEmptyIntersect = true;
        TYPE intersectType = null;
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
    public TYPE[] getOutputTypes() {
        return supportedTypes;
    }

    @Override
    public IMediator getMediator(TYPE type) {
        if (type == TYPE.BYTE_ARRAY) {
           return () ->  {
                    synchronized (this) {
                        byte[] out;
                        do {
                            out = packages.get(outPackageNumber);
                        }
                        while (out == null); // waiting package

                        if (out.length == 0) {
                            return null;
                        }
                        packages.remove(outPackageNumber);
                        ++outPackageNumber;
                        return out;
                    }
            };
        }
        else
            return null;
    }

    @Override
    public void runThreads() {
        for (int i = 0; i < numOfThreads; ++i) {
            executorsThreads.add(new Thread(new RLEExecutorRunnable(bufferSize, mode, mediator), "RLEExecutor/Thread" + i));
            executorsThreads.get(i).start();
        }
    }

    @Override
    public void joinThreads() throws InterruptedException {
        for (Thread executorThread : executorsThreads) {
            executorThread.join();
        }
    }

    @Override
    public RC getRC() {
        return rc;
    }

    @Override
    public void reportError() {
        isConsumerError = true;
    }
}
