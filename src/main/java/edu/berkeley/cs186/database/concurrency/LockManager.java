package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we recommend you implement.
        // You're free to modify their type signatures, delete, or ignore them.


        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.1
         */

        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock granted : locks) {
                if (granted.transactionNum != except) {
                    if (LockType.compatible(lockType, granted.lockType) == false) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            List<Lock> lockTemp = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<>());

            if (getTransactionLockType(lock.transactionNum) != LockType.NL) {
                for (Lock temp : lockTemp) {
                    if (temp.name == lock.name) {
                        //lockTemp.remove(temp);
                        lockTemp.add(lock);
                        transactionLocks.put(lock.transactionNum, lockTemp);
                        break;
                    }
                }
                for (Lock temp : locks) {
                    if (temp.transactionNum == lock.transactionNum) {
                        //locks.remove(temp);
                        locks.add(lock);
                        break;
                    }
                }
            }

            else {
                locks.add(lock);
                lockTemp.add(lock);
                transactionLocks.put(lock.transactionNum, lockTemp);
            }
            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            List<Lock> lockTemp = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<>());
            locks.remove(lock);
            lockTemp.remove(lock);

            if (!lockTemp.isEmpty()) {
                transactionLocks.put(lock.transactionNum, lockTemp);
            }

            else {
                transactionLocks.remove(lock.transactionNum);
            }

            processQueue();

            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            TransactionContext temp = request.transaction;
            if (addFront) {
                waitingQueue.addFirst(request);
            }
            else {
                waitingQueue.addLast(request);
            }
            temp.prepareBlock();
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement

            if (!requests.hasNext()) {
                return;
            }

            LockRequest req = requests.next();

            if (checkPromote && !checkCompatible(req.lock.lockType,-1)) {
                promote(req.transaction, req.lock.name, req.lock.lockType);
                checkPromote = false;
                waitingQueue.removeFirst();
                req.transaction.unblock();
            }

            if (!checkPromote && checkCompatible(req.lock.lockType, -1)) {
                grantOrUpdateLock(req.lock);
                waitingQueue.removeFirst();

                if (req.releasedLocks.isEmpty()) {
                    req.transaction.unblock();
                }

                else {
                    while (req.releasedLocks.isEmpty()) {
                        for (Lock i : req.releasedLocks) {
                            ResourceEntry resourceEntry = getResourceEntry(i.name);

                            getResourceEntry(i.name).locks.remove(i);

                            List<Lock> temp = transactionLocks.getOrDefault(i.transactionNum, new ArrayList<>());
                            temp.remove(i);
                            if (temp.isEmpty()) {
                                transactionLocks.remove(i.transactionNum);
                            }
                            else {
                                transactionLocks.put(i.transactionNum, temp);
                            }

                            if (resourceEntry.checkCompatible(req.lock.lockType, -1)) {
                                resourceEntry.grantOrUpdateLock(req.lock);
                                resourceEntry.waitingQueue.removeFirst();

                            }
                        }
                        req.transaction.unblock();
                    }
                }

            }

            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock granted : locks) {
                if(granted.transactionNum == transaction) {
                    return granted.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            if (getLockType(transaction, name) == lockType && !releaseNames.contains(name)) {
                throw new DuplicateLockRequestException("Error for duplicate");
            }

            ResourceEntry resourceEntry = getResourceEntry(name);
            Long num = transaction.getTransNum();
           //List<Lock> transTemp = transactionLocks.get(num);
            Lock lock;
            lock = new Lock(name, lockType, num);
            //mo
            if (resourceEntry.checkCompatible(lockType, -1) && resourceEntry.waitingQueue.isEmpty()) {
                resourceEntry.grantOrUpdateLock(lock);
                if (releaseNames.contains(lock.name)) {
                    releaseNames.remove(lock.name);
                }
            }
            else {
                shouldBlock = true;
            }

            if (shouldBlock) {
                List<Lock> lockTemp = resourceEntry.locks;
                lock = new Lock(name, lockType, num);
                //getLock
                Lock temp = null;
                for (Lock i : lockTemp) {
                    if (i.transactionNum == num) {
                        temp = i;
                    }
                }
                if (temp == null || temp.lockType == lockType) {
                    resourceEntry.addToQueue(new LockRequest(transaction, lock), true);
                }
                else {
                    resourceEntry.addToQueue(new LockRequest(transaction, lock), false);
                    lockTemp.remove(temp);
                    if (resourceEntry.checkCompatible(lockType, -1)) {
                        temp.lockType = lockType;
                        lockTemp.add(lock);
                    }
                    else {
                        lockTemp.add(temp);
                        List<Lock> lockTemp2 = new ArrayList<>();
                        lockTemp2.add(temp);
                        resourceEntry.waitingQueue.addFirst(new LockRequest(transaction, lock, lockTemp2));
                    }
                    shouldBlock= false;
                }

            }

            else {
                for (ResourceName temp: releaseNames) {
                    if (getLockType(transaction, temp) == LockType.NL) {
                        throw new NoLockHeldException("Error for no held");
                    }
                    else {
                        ResourceEntry tempResourceEntry = getResourceEntry(temp);
                        List<Lock> reTemp = tempResourceEntry.locks;
                        Lock tempTemp = null;
                        for (Lock i : reTemp) {
                            if (i.transactionNum == num) {
                                tempTemp = i;
                            }
                        }
                        if (temp != null) {
                            tempResourceEntry.releaseLock(tempTemp);
                        }

                    }
                }
            }


        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */

    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            if (getLockType(transaction, name) == lockType) {
                throw new DuplicateLockRequestException("Error for duplicate");
            }
            ResourceEntry resourceEntry = getResourceEntry(name);
            Long num = transaction.getTransNum();
            Lock lock;
            lock = new Lock(name, lockType, num);
            if (resourceEntry.checkCompatible(lockType, -1) && resourceEntry.waitingQueue.isEmpty()) {
                resourceEntry.grantOrUpdateLock(lock);
            }
            else {
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), false);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            if (getLockType(transaction, name) == LockType.NL) {
                throw new NoLockHeldException("Error for no held");
            }
            ResourceEntry resourceEntry = getResourceEntry(name);
            Long num = transaction.getTransNum();
            List<Lock> listTemp = resourceEntry.locks;
            Lock temp = null;
            for (Lock i : listTemp) {
                if (i.transactionNum == num) {
                    temp = i;
                }
            }
            if (temp != null) {
                resourceEntry.releaseLock(temp);
            }

        }
        //transaction.unblock();
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    boolean checkPromote = false;
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        //boolean checkPromote = false;
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            Long num = transaction.getTransNum();
            List<Lock> listTemp = resourceEntry.locks;
            Lock lock;
            lock = new Lock(name, newLockType, num);
            Lock temp = null;
            for (Lock i : listTemp) {
                if (i.transactionNum == num) {
                    temp = i;
                }
            }
            if (temp == null) {
                throw new NoLockHeldException("Error for no held");
            }

            if (!LockType.substitutable(newLockType, temp.lockType)) {
                throw new InvalidLockException("Error for in vaild");
            }

            if (temp.lockType == newLockType) {
                throw new DuplicateLockRequestException("Error for duplicate");
            }

            listTemp.remove(temp);

            if (resourceEntry.checkCompatible(newLockType, -1)) {
                temp.lockType = newLockType;
                listTemp.add(temp);
                return;
            }

            checkPromote = true;
            listTemp.add(temp);
            List<Lock> lockTemp2 = new ArrayList<>();
            lockTemp2.add(temp);
            resourceEntry.waitingQueue.addFirst(new LockRequest(transaction, lock, lockTemp2));
            transaction.prepareBlock();
        }

        if (shouldBlock) {
            transaction.block();
        }
        else {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        if (resourceEntry.locks.isEmpty()) {
            return LockType.NL;
        }
        for (Lock temp : resourceEntry.locks) {
            if (temp.transactionNum == transaction.getTransNum()) {
                return temp.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
