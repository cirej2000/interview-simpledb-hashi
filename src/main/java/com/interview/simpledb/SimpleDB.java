package com.interview.simpledb;

public interface SimpleDB {
    // Set the key to given value
    void Set(String key, Integer value);

    // Get the value for the given key. Returns null if value does not exist
    Integer Get(String key);

    // Unset the key, making it just like that key was never set
    void Unset(String key);

    // Begin opens a new transaction
    void Begin();

    // Commit closes all open transaction blocks, permanently apply the
    // changes made in them.
    void Commit() throws Exception;

    // Rollback undoes all fo the commands issued in the most recent
    // transaction block, and closes the block.
    void Rollback() throws Exception;
}
