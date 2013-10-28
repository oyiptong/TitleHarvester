package org.mozilla.up;

import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import mockit.*;
import static org.testng.AssertJUnit.*;

/**
 * Created with IntelliJ IDEA.
 * User: oyiptong
 * Date: 10/29/2013
 * Time: 12:17 PM
 */
public class TitleHarvesterMapperGetTitleTest
{
    @Mocked Mapper.Context mockedContext;
    @Mocked Counter mockCounter;
    TitleHarvester.TitleHarvestMapper thm;
    Method method;

    @BeforeMethod
    public void setUp() throws Exception
    {
        thm = new TitleHarvester.TitleHarvestMapper();
        method = TitleHarvester.TitleHarvestMapper.class.getDeclaredMethod("getTitleKeywords", String.class, Mapper.Context.class);
        method.setAccessible(true);
    }

    @Test
    public void testMapperGetTitleKeywordsEmptyInput() throws Exception
    {
        new NonStrictExpectations()
        {{
                mockedContext.getCounter(TitleHarvester.ParseStats.DATA_NO_JSON); result = mockCounter;
                mockedContext.getCounter(TitleHarvester.PageHTTPStatus.UNDEFINED); result = mockCounter;
                mockedContext.getCounter(TitleHarvester.PageHTTPStatus.REDIRECTION_3XX); result = mockCounter;
                mockCounter.increment(anyInt);
        }};

        String[] titleKeywords;

        titleKeywords = (String[]) method.invoke(thm, null, mockedContext);
        assertEquals(null, titleKeywords);

        titleKeywords = (String[]) method.invoke(thm, "", mockedContext);
        assertEquals(null, titleKeywords);

        titleKeywords = (String[]) method.invoke(thm, " ", mockedContext);
        assertEquals(null, titleKeywords);

        titleKeywords = (String[]) method.invoke(thm, "{}", mockedContext);
        assertEquals(null, titleKeywords);

        titleKeywords = (String[]) method.invoke(thm, "{\"http_result\":302}", mockedContext);
        assertEquals(null, titleKeywords);
    }

    @Test
    public void testMapperGetTitleKeywordsNoTitle() throws Exception
    {
        new NonStrictExpectations() {{
            mockedContext.getCounter(TitleHarvester.PageHTTPStatus.SUCCESS_2XX); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "undefined"); result = mockCounter;
            mockedContext.getCounter(TitleHarvester.ParseStats.PAGE_NO_TITLE); result = mockCounter;
            mockCounter.increment(anyInt);
        }};

        String[] inputs = new String[]
        {
                "{\"http_result\":200}",
                "{\"http_result\":200, \"content\": {\"title\": \"\"}}",
                "{\"http_result\":200, \"content\": {\"title\": \" \"}}"
        };

        String[] titleKeywords;
        for (String json : inputs)
        {
            titleKeywords = (String[]) method.invoke(thm, json, mockedContext);
            assertEquals(null, titleKeywords);
        }
    }

    @Test
    public void testMapperGetTitleKeywordsNoKeywords() throws Exception
    {
    	new NonStrictExpectations() {{
            mockedContext.getCounter(TitleHarvester.PageHTTPStatus.SUCCESS_2XX); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "undefined"); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "html-doc"); result = mockCounter;
            mockedContext.getCounter(TitleHarvester.ParseStats.PAGE_NO_KEYWORDS); result = mockCounter;
            mockCounter.increment(anyInt);
        }};
        
        String json = "{\"http_result\":200, \"content\": {\"title\": \"AwesomeTitle\", \"type\": \"html-doc\"}}";
        String[] titleKeywords = (String[]) method.invoke(thm, json, mockedContext);
        assertEquals("AwesomeTitle", titleKeywords[0]);
        assertEquals(null, titleKeywords[1]);
    }
    @Test
    public void testMapperGetTitleKeywords() throws Exception
    {
        new NonStrictExpectations() {{
            mockedContext.getCounter(TitleHarvester.PageHTTPStatus.SUCCESS_2XX); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "undefined"); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "html-doc"); result = mockCounter;
            mockedContext.getCounter(TitleHarvester.ParseStats.PAGE_NO_KEYWORDS); result = mockCounter;
            mockCounter.increment(anyInt);
        }};

        String json = "{\"http_result\":200, \"content\": {\"title\": \"AwesomeTitle\", \"type\": \"html-doc\", \"meta_tags\": [{\"name\": \"keywords\", \"content\": \"AwesomeKeywords\"}]}}";
        String[] titleKeywords = (String[]) method.invoke(thm, json, mockedContext);
        assertEquals("AwesomeTitle", titleKeywords[0]);
        assertEquals("AwesomeKeywords", titleKeywords[1]);
    }
}
