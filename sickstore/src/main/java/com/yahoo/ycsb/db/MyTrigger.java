package com.yahoo.ycsb.db;

import ch.qos.logback.core.joran.spi.NoAutoStart;
import ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP;
import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.Compressor;
import ch.qos.logback.core.rolling.helper.FileNamePattern;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Friedrich on 20.09.2016.
 */
@NoAutoStart
public class MyTrigger<E> extends SizeAndTimeBasedFNATP<E>
{
  private final AtomicBoolean trigger = new AtomicBoolean();

  public boolean isTriggeringEvent(final File activeFile, final E event) {
    if (trigger.compareAndSet(false, true) && activeFile.length() > 0) {
      String maxFileSize = getMaxFileSize();
      setMaxFileSize("1"); //WHAT???
      long time = getCurrentTime();
      setDateInCurrentPeriod(time);
      FileNamePattern fileNamePattern = new FileNamePattern(Compressor.computeFileNameStr_WCS(tbrp.getFileNamePattern(), CompressionMode.NONE), context);
      elapsedPeriodsFileName = fileNamePattern.convertMultipleArguments(dateInCurrentPeriod, 0);
      super.isTriggeringEvent(activeFile, event);
      setMaxFileSize(maxFileSize);
      return true;
    }
    return super.isTriggeringEvent(activeFile, event);
  }
}



