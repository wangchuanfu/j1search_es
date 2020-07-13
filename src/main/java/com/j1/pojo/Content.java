package com.j1.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Content {
	private String sku;
	private String title;
	private String img;
	private String imgLazy;
	private String price;
	private String shop;

}
