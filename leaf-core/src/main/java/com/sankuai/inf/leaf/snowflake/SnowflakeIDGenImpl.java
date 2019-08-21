package com.sankuai.inf.leaf.snowflake;

import com.google.common.base.Preconditions;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SnowflakeIDGenImpl implements IDGen {

    @Override
    public boolean init() {
        return true;
    }

    static private final Logger LOGGER = LoggerFactory.getLogger(SnowflakeIDGenImpl.class);

    private final long twepoch = 1288834974657L;
    private final long workerIdBits = 10L;
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);//最大能够分配的workerid =1023
    private final long sequenceBits = 12L;
    private final long workerIdShift = sequenceBits;
    private final long timestampLeftShift = sequenceBits + workerIdBits;
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);
    private long workerId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;
    public boolean initFlag = false;
    private static final Random RANDOM = new Random();
    private int port;

    public SnowflakeIDGenImpl(String zkAddress, int port) {
        this.port = port;
        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(Utils.getIp(), String.valueOf(port), zkAddress);
        initFlag = holder.init();
        if (initFlag) {
            workerId = holder.getWorkerID();
            LOGGER.info("START SUCCESS USE ZK WORKERID-{}", workerId);
        } else {
            Preconditions.checkArgument(initFlag, "Snowflake Id Gen is not init ok");
        }
        Preconditions.checkArgument(workerId >= 0 && workerId <= maxWorkerId, "workerID must gte 0 and lte 1023");
    }

    @Override
    public synchronized Result get(String key) {
        long timestamp = timeGen();
        //当前时间 < 上次获取时间，代表发生时钟回拨
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            //如果在5ms内，则进行双倍时间wait等待，大于5ms直接抛出异常
            if (offset <= 5) {
                try {
                    wait(offset << 1);
                    timestamp = timeGen();
                    //等待后仍然出现回拨，直接报错
                    if (timestamp < lastTimestamp) {
                        return new Result(-1, Status.EXCEPTION);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("wait interrupted");
                    return new Result(-2, Status.EXCEPTION);
                }
            } else {
                return new Result(-3, Status.EXCEPTION);
            }
        }
        //如果时间一致，代表着在同一时间 同一毫秒内并发访问，
        if (lastTimestamp == timestamp) {
            //sequenceMask = -1L ^ (-1L << sequenceBits);代表着2的12次方，4096，进行与位运算
            sequence = (sequence + 1) & sequenceMask;
            //sequence == 0代表着从 随机值到4096 的数字已经被用完了，需要等待到下一毫秒再获取
            if (sequence == 0) {
                //seq 为0的时候表示是下一毫秒时间开始对seq做随机
                sequence = RANDOM.nextInt(100);
                //等待到下一毫秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            //如果是新的ms开始
            sequence = RANDOM.nextInt(100);
        }
        lastTimestamp = timestamp;
        // 0 41 10 12
        //时间左移22 workerId左移12，进行或位运算，或运算 有一个有1就是1 否则是0 其实就是原样输出并拼接
        /**
         * 0100 1011 0000 0101 0010 0011 0100 0000 1111 0000 0100 0000 0000 0000 0000 0000
         * 0100 1011 0000 0101 0010 0011 0100 0000 1111 0000 0100 0000 0001 0000 0000 0000
         * 0100 1011 0000 0101 0010 0011 0100 0000 1111 0000 0100 0000 0001 0000 0000 0001
         */
        long id = ((timestamp - twepoch) << timestampLeftShift) | (workerId << workerIdShift) | sequence;
        return new Result(id, Status.SUCCESS);

    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

}
