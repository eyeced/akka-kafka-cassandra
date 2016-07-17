package org.learn.akka.data;

import java.util.Date;

/**
 * Created by abhiso on 6/18/16.
 */
public class Reading {

    private Long deviceId;
    private Double value;
    private Date readTime;
    private Long flag;

    public Reading() {}

    public Reading(Long deviceId, Double value, Date readTime, Long flag) {
        this.deviceId = deviceId;
        this.value = value;
        this.readTime = readTime;
        this.flag = flag;
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public Double getValue() {
        return value;
    }

    public Date getReadTime() {
        return readTime;
    }

    public Long getFlag() {
        return flag;
    }

    @Override
    public String toString() {
        return "Reading{" +
                "deviceId=" + deviceId +
                ", value=" + value +
                ", readTime=" + readTime +
                ", flag=" + flag +
                '}';
    }
}
