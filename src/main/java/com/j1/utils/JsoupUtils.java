package com.j1.utils;

import com.j1.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.util.ArrayList;
//@Service
@Component
public class JsoupUtils {
    public static void main(String[] args) throws Exception {

         new JsoupUtils().getJdDataByJsoup ( "java" ).forEach ( System.out::println );
    }

    public    ArrayList<Content> getJdDataByJsoup(String keyWord) throws Exception {

        ArrayList <Content>list=new ArrayList ( );
        //爬取京东数据,
        String url = "https://search.jd.com/Search?keyword="+keyWord;

        Document document = Jsoup.parse ( new URL ( url), 3000 );

        Element element = document.getElementById ( "J_goodsList" );
        Elements lements = element.getElementsByTag ( "li" );

        for (Element el : lements) {
            Content content=new Content();
            String sku = el.attributes().get("data-sku");

            String img = el.getElementsByTag("img").attr("src");
            String dataUrl = el.getElementsByTag("a").attr("href");

            String imgLazy = el.getElementsByTag("img").attr("source-data-lazy-img");

            String price = el.getElementsByClass("p-price").text();

            String title = el.getElementsByClass("p-name").text();

            String shop = el.getElementsByClass("J_im_icon").text();


            content.setSku(sku);
            content.setTitle(title);
            content.setImg(img);
            content.setImgLazy(imgLazy);
            content.setPrice(price);
            content.setShop(shop);

            list.add(content);


        }
        return list;
    }


}
