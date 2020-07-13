package com.j1.action;

import com.j1.service.InitIndexService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * Created by wangchuanfu on 20/7/10.
 */
//初始化索引,数据
@RestController

public class InitIndexAction {
        //将爬取到的数据 推送到es中
    @Resource
    InitIndexService initIndexService;

    @RequestMapping("/initIndex/{keyword}")
   public String initIndex(@PathVariable("keyword") String keyword) throws  Exception{
      Boolean b= initIndexService.initIndex (keyword);
      if(b){
          return "initIndex success";
      }
      return "initIndex false";

   }
    @RequestMapping("/hello")
    public String helloSpringBoot(@RequestParam(value = "userName") String userName) throws Exception{

        return "hello hot "+userName;
    }

}
