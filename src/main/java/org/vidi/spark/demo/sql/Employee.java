package org.vidi.spark.demo.sql;

import java.io.Serializable;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class Employee implements Serializable {

    private static final long serialVersionUID = -422071446486457663L;

    private String name;
    private int age;
    private int depId;
    private String gender;
    private int salary;

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

    public int getDepId() {
        return depId;
    }

    public void setDepId(int depId) {
        this.depId = depId;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "name='" + name +
                ", age=" + age +
                ", depId=" + depId +
                ", gender='" + gender +
                ", salary=" + salary +
                '}';
    }
}
