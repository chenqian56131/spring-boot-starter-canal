package com.xpand.starter.canal.annotation;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.annotation.*;

/**
 * used to indicate that method(or methods) is(are) the candidate of the
 * canal event distributor
 *
 * @author chen.qian
 * @date 2018/3/19
 */

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ListenPoint {

    /**
     * canal destination
     * default for all
     * @return canal destination
     */
    String destination() default "";

    /**
     * database schema which you are concentrate on
     * default for all
     * @return canal destination
     */
    String[] schema() default {};

    /**
     * tables which you are concentrate on
     * default for all
     * @return canal destination
     */
    String[] table() default {};

    /**
     * canal event type
     * default for all
     * @return canal event type
     */
    CanalEntry.EventType[] eventType() default {};

}
