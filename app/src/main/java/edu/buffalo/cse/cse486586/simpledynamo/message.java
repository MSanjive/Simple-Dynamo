package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.Map;


public class message implements Serializable
{
    public String key;
    public String value;
    public String originalport;
    public Integer op_type;
    public Map<String, String> key_val;

    public message(String k, String v, String my, Integer o, Map<String, String> kv)
    {
        key = k;
        value = v;
        originalport = my;
        op_type = o;
        key_val = kv;
    }
}
