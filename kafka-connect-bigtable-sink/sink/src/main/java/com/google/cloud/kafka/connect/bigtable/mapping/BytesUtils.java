/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.kafka.connect.bigtable.mapping;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This code is copied from hbase-common using Apache 2.0 License
 */
public class BytesUtils {

  public static final long BYTE_ARRAY_BASE_OFFSET;

  private static final String UTF8_CSN;

  static {
    BYTE_ARRAY_BASE_OFFSET = -1L;
    UTF8_CSN = StandardCharsets.UTF_8.name();
  }

  public static byte[] toBytes(double d) {
    return toBytes(Double.doubleToRawLongBits(d));
  }

  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset,
      int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  public static byte[] toBytes(int val) {
    byte[] b = new byte[4];

    for (int i = 3; i > 0; --i) {
      b[i] = (byte) val;
      val >>>= 8;
    }

    b[0] = (byte) val;
    return b;
  }

  public static byte[] toBytes(short val) {
    byte[] b = new byte[]{0, (byte) val};
    val = (short) (val >> 8);
    b[0] = (byte) val;
    return b;
  }

  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + 4];
    int offset = putInt(result, 0, val.scale());
    putBytes(result, offset, valueBytes, 0, valueBytes.length);
    return result;
  }

  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < 4) {
      throw new IllegalArgumentException(
          "Not enough room to put an int at offset " + offset + " in a " + bytes.length
              + " byte array");
    } else {
      return ConverterHolder.BEST_CONVERTER.putInt(bytes, offset, val);
    }
  }


  public static byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException var2) {
      throw new IllegalArgumentException("UTF8 decoding is not supported", var2);
    }
  }

  public static byte[] toBytes(boolean b) {
    return new byte[]{(byte) (b ? -1 : 0)};
  }

  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];

    for (int i = 7; i > 0; --i) {
      b[i] = (byte) ((int) val);
      val >>>= 8;
    }

    b[0] = (byte) ((int) val);
    return b;
  }

  static class ConverterHolder {

    static final String UNSAFE_CONVERTER_NAME =
        ConverterHolder.class.getName() + "$UnsafeConverter";
    static final Converter BEST_CONVERTER = getBestConverter();

    ConverterHolder() {
    }

    static Converter getBestConverter() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_CONVERTER_NAME);
        Converter converter = (Converter) theClass.getConstructor().newInstance();
        return converter;
      } catch (Throwable var2) {
        return BytesUtils.ConverterHolder.PureJavaConverter.INSTANCE;
      }
    }

    abstract static class Converter {

      Converter() {
      }

      abstract long toLong(byte[] var1, int var2, int var3);

      abstract int putLong(byte[] var1, int var2, long var3);

      abstract int toInt(byte[] var1, int var2, int var3);

      abstract int putInt(byte[] var1, int var2, int var3);

      abstract short toShort(byte[] var1, int var2, int var3);

      abstract int putShort(byte[] var1, int var2, short var3);
    }

    protected static final class PureJavaConverter extends Converter {

      static final ConverterHolder.PureJavaConverter INSTANCE = new ConverterHolder.PureJavaConverter();

      private PureJavaConverter() {
      }

      long toLong(byte[] bytes, int offset, int length) {
        long l = 0L;

        for (int i = offset; i < offset + length; ++i) {
          l <<= 8;
          l ^= (long) (bytes[i] & 255);
        }

        return l;
      }

      int putLong(byte[] bytes, int offset, long val) {
        for (int i = offset + 7; i > offset; --i) {
          bytes[i] = (byte) ((int) val);
          val >>>= 8;
        }

        bytes[offset] = (byte) ((int) val);
        return offset + 8;
      }

      int toInt(byte[] bytes, int offset, int length) {
        int n = 0;

        for (int i = offset; i < offset + length; ++i) {
          n <<= 8;
          n ^= bytes[i] & 255;
        }

        return n;
      }

      int putInt(byte[] bytes, int offset, int val) {
        for (int i = offset + 3; i > offset; --i) {
          bytes[i] = (byte) val;
          val >>>= 8;
        }

        bytes[offset] = (byte) val;
        return offset + 4;
      }

      short toShort(byte[] bytes, int offset, int length) {
        short n = 0;
        n = (short) ((n ^ bytes[offset]) & 255);
        n = (short) (n << 8);
        n ^= (short) (bytes[offset + 1] & 255);
        return n;
      }

      int putShort(byte[] bytes, int offset, short val) {
        bytes[offset + 1] = (byte) val;
        val = (short) (val >> 8);
        bytes[offset] = (byte) val;
        return offset + 2;
      }
    }

  }
}
