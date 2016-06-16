package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class IsExternalReferer extends EvalFunc<Integer> {

	private final String[] LIST_OF_SEARCH_ENGINE_REFERRERS = {
			"bing.",
			"search.ch",
			"shopping1.t-online.de",
			"advalvas.be",
			"tiscali.it",
			"www.toile.com",
			"nisearch.163.com",
			"startpagina.nl",
			"yahoo.com.jp",
			"yahoo.com",
			"toile.qc.ca",
			"webkatalog.lycos.de",
			"au.altavista.com",
			"libero.it",
			"news.baidu.com",
			"psearch.163.com",
			".pagina.nl",
			"orange.co.uk",
			"google",
			"alltheweb.com",
			"lycos.at",
			"yellowpages.com.au",
			"goeureka.com.au",
			"baidu.com",
			"search.tom.com",
			"vinden.nl",
			"tw.search.yahoo.com",
			"msn",
			"looksmart.com",
			"lycos.de",
			"yatv.com",
			"bigpond.com",
			"seek.3721.com",
			"sitesearch.tom.com",
			"lycos.nl",
			"aol.com",
			"ask.com",
			"aol.de",
			"wps.yam.com",
			"ww2.austronaut.at",
			"page.zhongsou.com",
			"cn.websearch.yahoo.com",
			"vindex.nl",
			"teoma",
			"ask.co.uk",
			"aol.co.uk",
			"dir.yam.com",
			"www.pchome.com.tw",
			"cha.iask.com",
			"go.8848.com",
			"zoeken.nl",
			"hotbot.com",
			"lycos.co.uk",
			"tw.imagesearch.yahoo.com",
			".ya.com",
			"austronaut.at",
			"search.sina.com.cn",
			"sogou.com",
			"ixquick.com",
			"altavista.com",
			"lycos.com",
			"images.aol.fr",
			"wanadoo.es",
			"dir.pchome.com.tw",
			"www.sogou.com/dir/",
			"yisou.com",
			"zoek.nl",
			"overture",
			"freenet.de",
			"lycos.fr",
			"voila.fr",
			"ilse.nl",
			"so.sohu.com",
			"cari.com.my",
			".naver.com",
			"wisenut",
			"shopping.freenet.de",
			"lycos.ca",
			"virgilio.it",
			"aon.at",
			"yehey.com",
			".alexa.com",
			"netscape",
			"t-online.de",
			"terra.es",
			"free.fr",
			"optonline.net",
			"freeserve",
			"tiscali.fr",
			"web.de",
			"bluewin.ch"
	};
	private final List<String> searchRefererList = Arrays.asList(LIST_OF_SEARCH_ENGINE_REFERRERS);

	public Integer exec(Tuple input) throws IOException {
		
		Integer returnval = 0;
		
		if (input == null || input.size() == 0)
			return null;

		String str = (String) input.get(0);  // this is the referer
		
		// Look for stuff in the domain, between the :// and the first /
		String domainStr = getDomainStr(str);
		
		returnval = isInExternalRefererList(returnval, domainStr);
		return returnval;
	}


	/**
	 * This version returns a Tuple for debugging purposes
	 */
//	public Tuple exec(Tuple input) throws IOException {
//		
//		TupleFactory tf = TupleFactory.getInstance();
//		Tuple returnTuple = tf.newTuple(3);
//		Integer returnval = 0;
//		
//		if (input == null || input.size() == 0)
//			return returnTuple;
//
//		String str = (String) input.get(0);  // this is the referer
//		
//		// Look for stuff in the domain, between the :// and the first /
//		String domainStr = getDomainStr(str);
//		
//		returnval = isInExternalRefererList(returnval, domainStr);
//		returnTuple.set(0, str);
//		returnTuple.set(1, domainStr);
//		returnTuple.set(2, returnval);
//		
//		return returnTuple;
//	}


	private Integer isInExternalRefererList(Integer returnval, String domainStr) {
		if (returnval == null || domainStr == null) return 0;
		for (String oneSearchStr : searchRefererList) {
			if (domainStr.contains(oneSearchStr)) returnval = 1;
		}
		return returnval;
	}
	
	protected String getDomainStr(String str) {
		if (str == null) return null;
		String domainString = null;
		int startIdx;
		if ((startIdx = str.indexOf("://")) > 0) { // find where the domain string starts
			int endIdx = str.indexOf("/", startIdx + 3);
			if (endIdx == -1) { // not found, so set it to str length
				endIdx = str.length();
			}
			if (endIdx > startIdx) { // do we have a legit string?
				domainString = str.substring(startIdx,endIdx);
				if (domainString != null && domainString.length() > 0) {
					System.out.println(domainString);
					return domainString;
				}
			}
		}
		return null;
	}

	public static void main(String[] args) {
		IsExternalReferer ier = new IsExternalReferer();
		String testVal = "http://toile.qc.ca/more";
		String domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));

		testVal = "http://toile.qc.ca";
		domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));

		testVal = "https://toile.qc.ca";
		domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));

		testVal = "veronica://toile.qc.ca";
		domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));

		testVal = "http://toile.qc.c";
		domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));
		
		testVal = "";
		domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));
		
		testVal = null;
		domainStr = ier.getDomainStr(testVal);
		System.out.println(domainStr);
		System.out.println(ier.isInExternalRefererList(0, domainStr));
	}
}
