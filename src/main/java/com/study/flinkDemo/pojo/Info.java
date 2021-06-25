package com.study.flinkDemo.pojo;

/**
 * @author feng xl
 * @date 2021/6/24 0024 19:56
 */

/**
 * 使用table api涉及到的实体类
 * */
public class Info {

    private int id;
    private String name;
    private int age;
    public Info(int id, String name, int age){
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Info{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
