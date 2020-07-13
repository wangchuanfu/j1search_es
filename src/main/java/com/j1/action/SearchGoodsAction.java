package com.j1.action;

import com.j1.service.InitIndexService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by wangchuanfu on 20/7/10.
 */

    @RestController
public class SearchGoodsAction {

    @Resource
    InitIndexService initIndexService;
    //根据关键字搜索
    @RequestMapping("/search/{keyword}/{pageNo}/{pageSize}")
    public  List searchPage(@PathVariable("keyword") String keyword,
                            @PathVariable("pageNo") Integer pageNo,
                            @PathVariable("pageSize") Integer pageSize) throws  Exception{
        if(keyword==null){
            keyword="java";//设置默认关键字
        }
        List<Map<String, Object>> list = initIndexService.search(keyword, pageNo, pageSize);

        return list;

    }

    //根据关键字查询
    @RequestMapping("/search/suggest")
    public List<String> searchSuggest(@RequestParam(required = false) String keyword) {
        if (StringUtils.isBlank(keyword)) {
            return new ArrayList<>(Arrays.asList("Welcome", "Hello"));
        }
        return new ArrayList<>(Arrays.asList(keyword, keyword + "怎么样"));
    }
}
