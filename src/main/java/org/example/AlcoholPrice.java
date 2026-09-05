package org.example;


public class AlcoholPrice {
    private String brand;
    private double price;
    private String country;

    public AlcoholPrice(){

    }

    public String getBrand() {return brand;}
    public void setBrand(String brand) {this.brand = brand;}

    public double getPrice() {return price;}
    public void setPrice(double price) {this.price = price;}

    public String getCountry() {return country;}
    public void setCountry(String country) {this.country = country;}

    @Override
    public String toString() {
        return "AlcoholPrice{" + "brand=" + brand + ", price=" + price + ", country=" + country + '}';
    }
}
