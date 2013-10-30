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
        method = TitleHarvester.TitleHarvestMapper.class.getDeclaredMethod("getTitle", String.class, Mapper.Context.class);
        method.setAccessible(true);
    }

    @Test
    public void testMapperGetTitleEmptyInput() throws Exception
    {
        new NonStrictExpectations()
        {{
                mockedContext.getCounter(TitleHarvester.ParseStats.DATA_NO_JSON); result = mockCounter;
                mockedContext.getCounter(TitleHarvester.PageHTTPStatus.UNDEFINED); result = mockCounter;
                mockedContext.getCounter(TitleHarvester.PageHTTPStatus.REDIRECTION_3XX); result = mockCounter;
                mockCounter.increment(anyInt);
        }};

        String title;

        title = (String) method.invoke(thm, null, mockedContext);
        assertEquals(null, title);

        title = (String) method.invoke(thm, "", mockedContext);
        assertEquals(null, title);

        title = (String) method.invoke(thm, " ", mockedContext);
        assertEquals(null, title);

        title = (String) method.invoke(thm, "{}", mockedContext);
        assertEquals(null, title);

        title = (String) method.invoke(thm, "{\"http_result\":302}", mockedContext);
        assertEquals(null, title);
    }

    @Test
    public void testMapperGetTitleNoTitle() throws Exception
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

        String title;
        for (String json : inputs)
        {
            title = (String) method.invoke(thm, json, mockedContext);
            assertEquals(null, title);
        }
    }

    @Test
    public void testMapperGetTitle() throws Exception
    {
        new NonStrictExpectations() {{
            mockedContext.getCounter(TitleHarvester.PageHTTPStatus.SUCCESS_2XX); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "undefined"); result = mockCounter;
            mockedContext.getCounter("DATA_PAGE_TYPE", "html-doc"); result = mockCounter;
            mockCounter.increment(anyInt);
        }};

        String json = "{\"http_result\":200, \"content\": {\"title\": \"AwesomeTitle\", \"type\": \"html-doc\"}}";
        String title = (String) method.invoke(thm, json, mockedContext);
        assertEquals("AwesomeTitle", title);
    }
}
