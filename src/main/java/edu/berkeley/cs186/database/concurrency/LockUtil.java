package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */

    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        LockType currentLockType = lockContext.lockman.getLockType(transaction, lockContext.name);
        if (currentLockType == (requestType)) {
            return;
        }
        switch(requestType) {
            case X:
                if (effectiveLockType != (LockType.X)) {
                    LockContext temp = lockContext.parentContext();
                    ArrayList<LockContext> temp1 = new ArrayList<>();
                    ArrayList<LockContext> temp2 = new ArrayList<>();
                    ArrayList<LockContext> temp3 = new ArrayList<>();
                    if (parentContext != null) {
                        if (lockTypeAnsDes(transaction, LockType.IS, lockContext, 0) || lockTypeAnsDes(transaction, LockType.S, lockContext, 0)) {

                            while (temp != null) {
                                if (temp.lockman.getLockType(transaction, temp.name) == (LockType.NL)) {
                                    temp1.add(temp);
                                }
                                if (temp.lockman.getLockType(transaction, temp.name) == (LockType.IS)) {
                                    temp2.add(temp);
                                }
                                if (temp.lockman.getLockType(transaction, temp.name) == (LockType.S)) {
                                    temp3.add(temp);
                                }
                                temp = temp.parentContext();
                            }

                            for (int i = temp2.size() -1; i >= 0; i--) {
                                temp2.get(i).promote(transaction, LockType.IX);
                            }

                            for (int i = temp3.size() -1; i >= 0; i--) {
                                temp3.get(i).promote(transaction, LockType.X);
                            }

                            if (temp3.size() == 0) {
                                for (int i = temp1.size() -1; i >= 0; i--) {
                                    temp1.get(i).acquire(transaction, LockType.IX);
                                }
                            }
                        }

                        else {

                            while (temp != null) {
                                if (temp.lockman.getLockType(transaction, temp.name) == (LockType.NL)) {
                                    temp2.add(temp);
                                }
                                temp = temp.parentContext();
                            }
                            for (int i = temp2.size() -1; i >= 0; i--) {
                                temp2.get(i).acquire(transaction, LockType.IX);
                            }
                        }
                    }

                    if (explicitLockType == (LockType.NL)) {
                        lockContext.acquire(transaction, LockType.X);
                    }

                    else {
                        if (effectiveLockType != (LockType.X)) {
                            lockContext.promote(transaction, LockType.X);
                        }
                        else {
                            lockContext.escalate(transaction);
                        }

                    }
                    break;
                }

            case S:
                if (effectiveLockType != (LockType.SIX) &&  effectiveLockType != (LockType.S) && effectiveLockType != (LockType.X)) {
                    LockContext temp = lockContext.parentContext();
                    if (parentContext != null) {
                        ArrayList<LockContext> temp2 = new ArrayList<>();

                        while (temp != null) {
                            if (temp.lockman.getLockType(transaction, temp.name) == (LockType.NL)) {
                                temp2.add(temp);
                            }
                            temp = temp.parentContext();
                        }

                        for (int i = temp2.size() -1; i > -1; i--) {
                            temp2.get(i).acquire(transaction, LockType.IS);
                        }

                    }
                    if (lockTypeAnsDes(transaction, LockType.IX, lockContext, 1) || lockContext.lockman.getLockType(transaction, lockContext.name) == (LockType.IX)) {
                        List<ResourceName> sisTemp = sisDescendants(transaction, lockContext);
                        sisTemp.add(lockContext.name);
                        lockContext.lockman.acquireAndRelease(transaction, lockContext.name, LockType.SIX, sisTemp);
                    }
                    else if (explicitLockType == (LockType.NL)) {
                        lockContext.acquire(transaction, LockType.S);
                    }
                    else {
                        lockContext.escalate(transaction);
                    }
                    break;
                }

            default:
                break;
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    public static boolean lockTypeAnsDes(TransactionContext transaction, LockType lockType, LockContext lockContext, int i) {
        List<Lock> temp = lockContext.lockman.getLocks(transaction);
        //de
        if (i == 1) {
            for (Lock temp2 : temp) {
                if (temp2.lockType == (lockType) && temp2.name.isDescendantOf(lockContext.name) && !lockContext.name.equals(temp2.name)) {
                    return true;
                }
            }
            return false;
        }
        //as
        else {
            for (Lock temp2 : temp) {
                if (temp2.lockType == (lockType) && lockContext.name.isDescendantOf(temp2.name) && !lockContext.name.equals(temp2.name)) {
                    return true;
                }
            }
            return false;
        }
    }

    //From context, it is private so update it in here
    public static List<ResourceName> sisDescendants(TransactionContext transaction, LockContext lockContext) {
        List<ResourceName> temp =  new ArrayList<>();
        List<Lock> temp3 = lockContext.lockman.getLocks(transaction);
        for (Lock temp2 : temp3) {
            if ((temp2.lockType == (LockType.S) || temp2.lockType == (LockType.IS)) && temp2.name.isDescendantOf(lockContext.name) && !temp2.name.equals(lockContext.name)) {
                temp.add(temp2.name);
            }
        }
        return temp;
    }

}
