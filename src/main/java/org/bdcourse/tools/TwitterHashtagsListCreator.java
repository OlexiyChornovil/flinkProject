package org.bdcourse.tools;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TwitterHashtagsListCreator {
    public List<String> getList() throws IOException {
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        Scanner s = new Scanner(new File(jobParameters.get("pathToList")));
        ArrayList<String> list = new ArrayList<String>();
        while (s.hasNext()){
            list.add(s.next());
        }
        s.close();
        return list;
    }
}
