package com.neoKV.neoKVclient;

import com.neoKV.network.AdminCommandType;
import com.neoKV.network.DataType;
import com.neoKV.network.payload.AdminCommandMessage;
import com.neoKV.network.payload.GetMessage;
import com.neoKV.network.payload.PutMessage;
import com.neoKV.network.utils.ByteBufferUtils;

import java.util.Scanner;

/**
 * @author neo82
 */
public class Console {
    private final NeoKVClient neoKVClient = new NeoKVClient();

    public void start() {
        this.neoKVClient.start();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            String s = scanner.nextLine();
            String[] words = s.split(" ");

            String command = words[0];

            if ("quit".equals(command)) {
                break;
            } else if ("admin".equals(command)) {
                String subCommand = words[1]; // flush ...etc...

                this.neoKVClient.sendData(AdminCommandMessage.of(AdminCommandType.findEnum(subCommand)));

            } else if ("get".equals(command)) {
                String key = words[1];

                this.neoKVClient.sendData(GetMessage.of(key));

            } else if ("set".equals(command)) {
                DataType dataType = getDataType(words);
                String key = getKey(words);
                String val = getVal(words);

                this.neoKVClient.sendData(PutMessage.of(dataType, key, ByteBufferUtils.getByteArrayBy(dataType, val)));
            }
        }
    }

    private DataType getDataType(String[] words) {
        return DataType.of(words[1]);
    }

    private String getKey(String [] words) {
        return words[2];
    }

    private String getVal(String [] words) {
        return words[3];
    }

    public static void main(String[] args) {
        Console console = new Console();
        console.start();
    }
}
