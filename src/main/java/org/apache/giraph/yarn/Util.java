package org.apache.giraph.yarn;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: dcapwell
 * Date: 11/10/11
 * Time: 6:39:12AM
 * To change this template use File | Settings | File Templates.
 */
public class Util {

    /**
     * Get the jar File path for the given class.  If the class is not jared, but in a .class file, this method
     * will return the directory containing the class
     */
    public static File getJarPath(Class jarClass) {
        return new File(jarClass.getProtectionDomain().getCodeSource().getLocation().getPath());
    }
}
