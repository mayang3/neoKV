package com.neoKV.network;

import io.netty.buffer.ByteBuf;

/**
 * 1. PUT <br>
 *  1-1. primitive type <br>
 *  header : [total_length(4byte)][transaction_id(16byte)][operation_code(1byte)] <br>
 *  payload : [key_length(4byte)][key(keyLength)][value_type(1byte)][value_length(4byte)][value] <br>
 *  <br>
 *  <br>
 *  1-2. collection type <br>
 *  header : [total_length(4byte)][transaction_id(16byte)][operation_code(1byte)] <br>
 *  payload : [key_length(4byte)][key(keyLength)][value_type(1byte)][value_size(4byte)]{[value_length(4byte)][value],[value_length(4byte)][value]...} <br>
 *  <br>
 *  <br>
 *  1-3. map type <br>
 *  header : [total_length(4byte)][transaction_id(16byte)][operation_code(1byte)] <br>
 *  payload : [key_length(4byte)][key(keyLength)][value_type(1byte)][value_size(4byte)]{[value_key_length(4byte)][value_key][value_value_length(4byte)][value_value], [value_key_length(4byte)][value_key][value_value_length(4byte)][value_value]...} <br>
 *  <br>
 *  <br>
 * 2. GET <br>
 *  header : [total_length(4byte)][transaction_id(16byte)][operation_code(1byte)] <br>
 *  payload : [keyLength(4byte)][key(keyLength)] <br>
 *  <br>
 *  <br>
 * 3. DELETE <br>
 *  header : [total_length(4byte)][transaction_id(16byte)][operation_code(1byte)] <br>
 *  payload : [keyLength(4byte)][key(keyLength)] <br>
 *
 * 4. RESPONSE_SUCCESS <br>
 * header : [total_length(4byte)][transaction_id(16byte)][operation_code(1byte)]
 * payload :
 */
public class Packet {
    private final ByteBuf buf;

    public Packet(ByteBuf buf) {
        this.buf = buf;
    }

    public int readInt() {
        return this.buf.readInt();
    }

    public String readString() {
        int size = this.buf.readInt();

        byte [] bytes = new byte[size];
        this.buf.readBytes(bytes);

        return new String(bytes);
    }

    public byte[] getAllBytes() {
        int length = this.buf.readableBytes();
        byte[] bytes = new byte[length];
        this.buf.readBytes(bytes);
        return bytes;
    }

    public byte readByte() {
        return this.buf.readByte();
    }

    public int writerIndex() {
        return this.buf.writerIndex();
    }

    public ByteBuf getBuf() {
        return this.buf;
    }

    public String readTransactionId() {
        byte [] bytes = new byte[36];
        this.buf.readBytes(bytes);

        return new String(bytes);
    }
}
