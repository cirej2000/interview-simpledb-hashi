package com.interview.simpledb;

import org.junit.Test;


import static org.junit.Assert.*;

public class SimpleDBTest {

  @Test
  public void testUnset() {
    final SimpleDB db = new SimpleDBImpl();
    db.Set("ex", 10);
    Integer result = db.Get("ex");
    assertNotNull(result);
    assertEquals(10, result.intValue());
    db.Unset("ex");
    result = db.Get("ex");
    assertNull(result);
  }


  @Test
  public void testRollBack() {
    final SimpleDB db = new SimpleDBImpl();
    db.Begin();
    db.Set("a", 10);

    Integer result = db.Get("a");
    assertNotNull(result);
    assertEquals(10, result.intValue());

    db.Begin();
    db.Set("a", 20);
    result = db.Get("a");
    assertNotNull(result);
    assertEquals(20, result.intValue());

    try {
      db.Rollback();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }

    // Expect value to be 10 after inner transaction is rolled back
    result = db.Get("a");
    assertNotNull(result);
    assertEquals(10, result.intValue());

    try {
      db.Rollback();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }

    // Key should not exist after outer transaction is rolled back
    result = db.Get("a");
    assertNull(result);
  }

  @Test
  public void testCommit() {
    final SimpleDB db = new SimpleDBImpl();
    db.Begin();
    db.Set("a", 30);
    db.Begin();
    db.Set("a", 40);
    try {
      db.Commit();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }

    Integer result = db.Get("a");
    assertNotNull(result);
    assertEquals(40, result.intValue());

    boolean caught = false;
    try {
      db.Commit();
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      fail("Expected commit to throw an exception when no transaction in progress");
    }
  }

  @Test
  public void testCommitRollback() {
    final SimpleDB db = new SimpleDBImpl();
    db.Begin();
    db.Set("a", 30);
    db.Begin();
    db.Set("a", 40);
    try {
      db.Commit();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }

    Integer result = db.Get("a");
    assertNotNull(result);
    assertEquals(40, result.intValue());

    boolean caught = false;
    try {
      db.Rollback();
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      fail("Expected rollback to throw an exception when no transaction in progress");
    }

    caught = false;
    try {
      db.Commit();
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      fail("Expected commit to throw an exception when no transaction in progress");
    }
  }

  @Test
  public void testMultiKeyTransactions() {

    final SimpleDB db = new SimpleDBImpl();

    db.Set("a", 10);
    db.Set("b", 10);

    db.Begin();
    assertEquals(10, db.Get("a").intValue());
    assertEquals(10, db.Get("b").intValue());
    db.Set("a", 20);
    assertEquals(20, db.Get("a").intValue());
    assertEquals(10, db.Get("b").intValue());

    db.Begin();
    assertEquals(20, db.Get("a").intValue());
    assertEquals(10, db.Get("b").intValue());
    db.Set("b", 30);
    assertEquals(20, db.Get("a").intValue());
    assertEquals(30, db.Get("b").intValue());

    try {
      db.Rollback();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertEquals(20, db.Get("a").intValue());
    assertEquals(10, db.Get("b").intValue());

    try {
      db.Rollback();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertEquals(10, db.Get("a").intValue());
    assertEquals(10, db.Get("b").intValue());

  }

  @Test
  public void testRollbackUnset() {

    final SimpleDB db = new SimpleDBImpl();

    db.Set("a", 10);
    assertEquals(10, db.Get("a").intValue());

    db.Begin();
    assertEquals(10, db.Get("a").intValue());
    db.Set("a", 20);
    assertEquals(20, db.Get("a").intValue());

    db.Begin();
    db.Unset("a");
    assertNull(db.Get("a"));

    try {
      db.Rollback();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertEquals(20, db.Get("a").intValue());

    try {
      db.Commit();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertEquals(20, db.Get("a").intValue());
  }

  @Test
  public void testCommitUnset() {

    final SimpleDB db = new SimpleDBImpl();

    db.Set("a", 10);

    db.Begin();
    assertEquals(10, db.Get("a").intValue());
    db.Unset("a");
    assertNull(db.Get("a"));

    try {
      db.Rollback();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertEquals(10, db.Get("a").intValue());

    db.Begin();
    db.Unset("a");
    assertNull(db.Get("a"));

    try {
      db.Commit();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertNull(db.Get("a"));

    db.Begin();
    assertNull(db.Get("a"));
    db.Set("a", 20);
    assertEquals(20, db.Get("a").intValue());

    try {
      db.Commit();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
    assertEquals(20, db.Get("a").intValue());
  }

}
