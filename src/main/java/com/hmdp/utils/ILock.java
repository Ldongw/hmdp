package com.hmdp.utils;

/**
 * @author A
 * @date 2025/8/31
 **/
public interface ILock {

    boolean tryLock(long timeoutSec);

    void unlock();
}
