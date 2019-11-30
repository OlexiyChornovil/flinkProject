package org.bdcourse.depricated.utils;

import java.util.*;
import java.util.stream.Collectors;

public class tools {


    public static HashSet<String> removeExcessElements(String[] array, Integer length)
    {
        //return (String[]) Stream.of(array).filter(array_element -> array_element.equals(element)).toArray();
        //return IntStream.of(array).filter(array_element -> array_element != element).toArray();
        List<String> wordsOfLengthTwo = Arrays.stream(array).filter(s -> s.length() == length).collect(Collectors.toList());
        return new HashSet<String>(wordsOfLengthTwo);
    }


    public static void main(String[] args){
        String[] data = "To be, or not to be,--that is the question:--".toLowerCase().split("\\W+");
        String element = "to";

        removeExcessElements(data, 2).forEach(System.out::println);




    }
}
