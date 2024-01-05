package com.neoKV.neoKVServer.protocol;

/**
 * @author neo82
 */
public class DataProtocol {

    // ** primitive type
    // [total_length(4byte)][tombstone(1byte)][key_length(4byte)][key(keyLength)][value_type(1byte)][value_length(4byte)][value]

    // ** collection type
    // [total_length(4byte)][tombstone(1byte)][key_length(4byte)][key(keyLength)][value_type(1byte)][value_size(4byte)]{[value_length(4byte)][value],[value_length(4byte)][value]...}

    // ** map type
    // [total_length(4byte)][tombstone(1byte)][key_length(4byte)][key(keyLength)][value_type(1byte)][value_size(4byte)]{[value_key_length(4byte)][value_key][value_value_length(4byte)][value_value], [value_key_length(4byte)][value_key][value_value_length(4byte)][value_value]...}
}
