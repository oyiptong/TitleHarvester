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
public class TitleHarvesterMapperTest
{
    @Mocked Mapper.Context mockedContext;
    @Mocked Counter mockCounter;
    TitleHarvester.TitleHarvestMapper thm;

    @BeforeMethod
    public void setUp() throws Exception
    {
        thm = new TitleHarvester.TitleHarvestMapper();
    }

    @AfterMethod
    public void tearDown() throws Exception
    {

    }

    @Test
    public void testMapperGetTitleEmpty() throws Exception
    {
        Method method = TitleHarvester.TitleHarvestMapper.class.getDeclaredMethod("getTitle", String.class, Mapper.Context.class);
        method.setAccessible(true);
        new NonStrictExpectations() {{
            mockedContext.getCounter(TitleHarvester.ParseStats.DATA_NO_JSON); result = mockCounter;
            mockCounter.increment(anyInt);
        }};
        String title;

        title = (String) method.invoke(thm, "", mockedContext);
        assertEquals(null, title);

        title = (String) method.invoke(thm, " ", mockedContext);
        assertEquals(null, title);

        title = (String) method.invoke(thm, null, mockedContext);
        assertEquals(null, title);
    }
}
