package main.dke.detectURLtopo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Printable {

    static String printable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ ";

    public int[][] convert(String url) {
        int[][] result = new int[1][75];

        if (url.length() < 75) {
            for (int i = 75 - url.length(), j = 0; i < 75; i++, j++)
                result[0][i] = printable.indexOf(url.charAt(j)) + 1;
            for (int i = 0; i < 74 - url.length(); i++)
                result[0][i] = 0;
        } else {
            for (int j = 0; j < 75; j++)
                result[0][j] = printable.indexOf(url.charAt(j + url.length() - 75)) + 1;
        }
        return result;
    }
}
