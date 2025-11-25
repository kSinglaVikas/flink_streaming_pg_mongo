package com.flink.streaming.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * User model with embedded orders support
 */
public class User implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private String name;
    private String email;
    private Integer age;
    private String city;
    private String createdAt;
    private String updatedAt;
    private List<Order> orders; // Embedded orders

    public User() {
        this.orders = new ArrayList<>();
    }

    public User(Integer id, String name, String email, Integer age, String city) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.age = age;
        this.city = city;
        this.orders = new ArrayList<>();
    }

    // Getters and Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }

    public void addOrder(Order order) {
        this.orders.add(order);
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", email='" + email + '\'' +
                ", age=" + age +
                ", city='" + city + '\'' +
                ", orders=" + orders.size() +
                '}';
    }
}
