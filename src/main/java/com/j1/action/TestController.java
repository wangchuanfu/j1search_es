package com.j1.action;
import com.j1.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
/**
 * Created by wangchuanfu on 20/7/13.
 */
@Slf4j
@RestController
public class TestController {

    @Autowired
    private RedisUtil redisUtil;

    @GetMapping("/testRedis")
    public Object getRedis() {
        redisUtil.set("name", "zhangsan");
        String name =redisUtil.get("name").toString();
        System.out.print(name);
        return redisUtil.get("name");
    }
}