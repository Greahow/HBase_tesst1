package com.blueasdfg;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }



    public Integer init(){
        return 1;
    }


    @Test
    public void result(){
        Integer a = init();
        System.out.println(a+1);
    }
}
