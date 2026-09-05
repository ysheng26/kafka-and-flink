package org.example;

public class CigarettePrice {
    private String brand;
    private double price;
    private String country;

    // just for demo, adding kafka meta here
    private String topic;
    private int partition;
    private long offset;

    public CigarettePrice(){}

    public String getBrand() {return brand;}
    public void setBrand(String brand) {this.brand = brand;}

    public double getPrice() {return price;}
    public void setPrice(double price) {this.price = price;}

    public String getCountry() {return country;}
    public void setCountry(String country) {this.country = country;}

    public long getOffset() {return offset;}
    public void setOffset(long offset) {this.offset = offset;}

    public int getPartition() {return partition;}
    public void setPartition(int partition) {this.partition = partition;}

    public String getTopic() {return topic;}
    public void setTopic(String topic) {this.topic = topic;}

    @Override
    public String toString() {
        return "CigarettePrice{" + "brand=" + brand + ", price=" + price + ", country=" + country + ", topic=" + topic + ", partition=" + partition + ", offset=" + offset + '}';
    }

}
