// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Flag {
  String name() default "";
  String usage() default "";
}
