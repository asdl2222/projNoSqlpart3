package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    private void readonlyChecking () {
        if (readonly) {
            throw new UnsupportedOperationException("context is not readonly");
        }
    }
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        readonlyChecking();

        if (getExplicitLockType(transaction) != LockType.NL) {
            throw new DuplicateLockRequestException("A lock is already held by the transcation");
        }

        LockType temp;
        if (parent == null) {
            temp = LockType.NL;
        }
        else {
            temp = parent.getExplicitLockType(transaction);
        }

        if (parent == null || LockType.substitutable(temp,LockType.parentLock(lockType))) {
            lockman.acquire(transaction,name,lockType);
            addDelChild(transaction, parent, 0);
        }
        else {
            throw new InvalidLockException("The request is invalid");
        }

        return;
    }
    public void addDelChild(TransactionContext transaction, LockContext parent, int i) {
        if (parent != null) {
            if (i == 0) {
                parent.numChildLocks.put(transaction.getTransNum(), parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
                addDelChild(transaction, parent.parent, 0);
            }
            else {
                parent.numChildLocks.put(transaction.getTransNum(), parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) - 1);
                addDelChild(transaction, parent.parent, 1);
            }
        }
        return;
    }
    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        readonlyChecking();
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("no lock on `name` is held by `transaction`");
        }
        if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) > 0) {
            throw new InvalidLockException("the lock cannot be released");
        }

        if (!numChildLocks.keySet().contains(transaction.getTransNum()) || numChildLocks.get(transaction.getTransNum()) == 0 ){
            lockman.release(transaction, name);
            addDelChild(transaction, parent, 1);
        }
        else {
            throw new InvalidLockException("the lock cannot be released");
        }

        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        readonlyChecking();
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("Transaction has no lock");
        }
        if (newLockType.equals(lockman.getLockType(transaction, name))) {
            throw new InvalidLockException("Aready lock on it");
        }
        if (newLockType.equals(LockType.SIX) && hasSIXAncestor(transaction)) {
            throw new InvalidLockException("SIX ancestor error");
        }
        List<ResourceName> temp = new ArrayList<>();
        temp.add(name);
        if (newLockType == LockType.SIX) {
            lockman.acquireAndRelease(transaction, name, newLockType, temp);
        }
        else {
            lockman.promote(transaction, name, newLockType);
        }

        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    private static LockType count;
    private static TransactionContext tempTrans;
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        readonlyChecking();
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("Transaction has no lock");
        }
        if (getExplicitLockType(transaction) == (LockType.S) || (getExplicitLockType(transaction) == (LockType.X) && numChildLocks.getOrDefault(transaction.getTransNum(), 0) == 0)) {
            return;
        }
        if (getExplicitLockType(transaction) == count && tempTrans.equals(transaction)) {
            return;
        }

        LockType lockTemp = null;
        List<Lock> locTempList = lockman.getLocks(transaction);
        List<ResourceName> releaseTemp = new ArrayList<>();
        for (Lock temp : locTempList) {
            if (temp.name.isDescendantOf(name) && !temp.name.equals(name) && (temp.lockType == (LockType.X) ||
                    temp.lockType == (LockType.IX) || temp.lockType == (LockType.SIX)) || (temp.name.equals(name) && (temp.lockType == (LockType.X) ||
                    temp.lockType == (LockType.IX) || temp.lockType == (LockType.SIX)))) {
                lockTemp = LockType.X;
            }
            else {
                lockTemp = LockType.S;
            }
            if (temp.name.equals(name) || temp.name.isDescendantOf(name)) {
                releaseTemp.add(temp.name);
            }
        }
        lockman.acquireAndRelease(transaction, name, lockTemp, releaseTemp);

        for (ResourceName temp : releaseTemp) {
            LockContext lockContext = fromResourceName(lockman, temp);
            delHelperFunc(transaction, lockContext);
            //addDelChild(transaction, lockContext, 1);
        }

        count = getExplicitLockType(transaction);
        tempTrans = transaction;

    }
    private void delHelperFunc(TransactionContext transaction, LockContext lockContext) {
        LockContext parentLockContext = lockContext.parentContext();
        if (parentLockContext != null) {
            Map<Long, Integer> temp = parentLockContext.numChildLocks;
            temp.put(transaction.getTransNum(), temp.get(transaction.getTransNum()) - 1);
            return;
        }
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockContext lockTemp = this;
        LockType temp;

        while (lockTemp != null) {
            temp = lockman.getLockType(transaction, lockTemp.name);

            if (temp != LockType.X && temp != LockType.S) {
                temp = LockType.NL;
            }

            if (temp != LockType.NL) {
                return temp;
            }
            lockTemp = lockTemp.parentContext();
        }

        return LockType.NL;

    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> temp = lockman.getLocks(transaction);
        for (Lock temp2 : temp) {
            if (temp2.lockType == (LockType.SIX) && name.isDescendantOf(temp2.name) && name != (temp2.name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    public List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> temp =  new ArrayList<>();
        List<Lock> temp3 = lockman.getLocks(transaction);
        for (Lock temp2 : temp3) {
            if ((temp2.lockType == (LockType.S) || temp2.lockType == (LockType.IS)) && temp2.name.isDescendantOf(name) && !temp2.name.equals(name)) {
                temp.add(temp2.name);
            }
        }
        return temp;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

