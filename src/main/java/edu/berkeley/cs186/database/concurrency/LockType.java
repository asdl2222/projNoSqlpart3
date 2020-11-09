package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        else if (a == NL) {
            return true;
        }
        else if (b == NL) {
            return true;
        }
        else if (a == IS) {
            if (b == X) {
                return false;
            }
            return true;
        }
        else if (a == IX) {
            if (b == IS || b == IX) {
                return true;
            }
            return false;
        }
        else if (a == S) {
            if (b == IS || b == S) {
                return true;
            }
            return false;
        }
        else if (a == SIX) {
            if (b == IS) {
                return true;
            }
            return false;
        }
        else if (a == X) {
            return false;
        }


        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        else if ((childLockType == S || childLockType == IS) && (parentLockType == IX || parentLockType == IS)) {
            return true;
        }
        else if ((childLockType == X || childLockType == IX || childLockType == SIX) && (parentLockType == IX || parentLockType == SIX)) {
            return true;
        }
        else if (childLockType == NL && parentLockType != null) {
            return true;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        else if (required == NL) {
            return true;
        }
        else if (required == substitute) {
            return true;
        }
        else if (required == S) {
            if (substitute == X || substitute == SIX) {
                return true;
            }
            return false;
        }
        else if (required == IX) {
            if (substitute == X || substitute == SIX) {
                return true;
            }
            return false;
        }
        else if (required == IS) {
            if (substitute == SIX || substitute == IX) {
                return true;
            }
            return false;
        }

        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

