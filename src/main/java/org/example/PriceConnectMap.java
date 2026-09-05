package org.example;

import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class PriceConnectMap implements CoMapFunction<CigarettePrice, AlcoholPrice, String> {
    @Override
    public String map1(CigarettePrice cigarettePrice) throws Exception {
        return cigarettePrice.toString();
    }

    @Override
    public String map2(AlcoholPrice alcoholPrice) throws Exception {
        return alcoholPrice.toString();
    }
}
