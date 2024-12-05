package com.interview.simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDBImpl implements SimpleDB{
    enum StatementName{
        SET,
        UNSET,
        GET,
    }
    class Statement {
        private String key;
        private Integer value;
        private StatementName statement;
        public Statement(StatementName statement, String key, Integer value){
            this.statement = statement;
            this.key = key;
            this.value = value;
        }
    }

    private Map<String, Integer> backingStore;

    private Stack<List<Statement>> rollbackStack = new Stack<>();
    private boolean isTransaction = false;
    private boolean isRollback = false;
    private List<Statement> preTransactionDBState;

    SimpleDBImpl(){
        backingStore=new ConcurrentHashMap<>();
        preTransactionDBState = new ArrayList<>();

    }



    @Override
    public void Set(String key, Integer value) {


        if (isTransaction && !isRollback){
            addTransaction(StatementName.SET, key, value);
            backingStore.put(key, value);
        }
        else {
            backingStore.put(key, value);
        }
    }

    private void addTransaction(StatementName name, String key, Integer value){
        List<Statement> l = rollbackStack.peek();
        Statement s = new Statement(StatementName.SET, key, value);
        l.add(s);
    }

    @Override
    public Integer Get(String key) {
        return backingStore.get(key);
    }


    @Override
    public void Unset(String key) {
        if (isTransaction && !isRollback){
            addTransaction(StatementName.UNSET, key, null);
            backingStore.remove(key);
        }
        else{
            backingStore.remove(key);
        }
    }

    @Override
    public void Begin() {
        if (!isTransaction && !backingStore.isEmpty() && preTransactionDBState.isEmpty()){
            preTransactionDBState = new ArrayList<>();
            for(String key : backingStore.keySet()){
                Statement s = new Statement(StatementName.SET, key, backingStore.get(key));
                preTransactionDBState.add(s);
            }
        }

        if (!isTransaction){
            isTransaction =true;
        }
        List<Statement> t = new ArrayList<>();
        rollbackStack.push(t);
    }

    @Override
    public void Commit() throws Exception {
        if (rollbackStack.isEmpty() || !isTransaction){
            throw new IllegalStateException("There is no transaction in progress (nothing to commit)!");
        }

        isTransaction =false;
        rollbackStack.clear();
        preTransactionDBState.clear();
    }

    @Override
    public void Rollback() throws Exception {

        if (rollbackStack.isEmpty()){
            throw new IllegalArgumentException("No transactions in progress!");
        }
        isRollback=true;

        if (preTransactionDBState.isEmpty()){
            backingStore.clear();
        }
        else{
            runStatements(preTransactionDBState);
        }

        //If we have only 1 transaction before rollback, we don't have anything to apply
        if (rollbackStack.size() == 1){
            rollbackStack.clear();
            return;
        }

        //otherwise, we'll pop the latest transaction and replay the previous
        rollbackStack.pop();
        runStatements(rollbackStack.peek());
        isRollback=false;//no longer in rollback mode, statements executed will be tracked isTransaction is true

        if (rollbackStack.isEmpty()){
            isTransaction =false;
            preTransactionDBState.clear();
        }
    }

    private void runStatements(List<Statement> transaction){
        for (int i=0; i< transaction.size(); i++) {
            switch (transaction.get(i).statement) {
                case SET:
                    Set(transaction.get(i).key, transaction.get(i).value);
                    break;
                case GET:
                    if (isRollback){
                        break;
                    }
                    Get(transaction.get(i).key);
                    break;
                case UNSET:
                    Unset(transaction.get(i).key);
                    break;
            }
        }
    }
}
