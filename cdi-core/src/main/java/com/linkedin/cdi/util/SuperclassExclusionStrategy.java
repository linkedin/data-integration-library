// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import java.lang.reflect.Field;


/**
 * This exclution strategy is used for excluding any field that is already defined in its superclass.
 * For example, given class A { field1 } and class B extends A { field1, field2 }, without an exclusion strategy
 * an IllegalStateException will be raised when serializing an instance of class B. With this exclusion strategy,
 * the serialization result would contain field2 from class B and field1 from class A
 */
public class SuperclassExclusionStrategy implements ExclusionStrategy
{
  public boolean shouldSkipClass(Class<?> arg0)
  {
    return false;
  }

  public boolean shouldSkipField(FieldAttributes fieldAttributes)
  {
    String fieldName = fieldAttributes.getName();
    Class<?> theClass = fieldAttributes.getDeclaringClass();

    return isFieldInSuperclass(theClass, fieldName);
  }

  private boolean isFieldInSuperclass(Class<?> subclass, String fieldName)
  {
    Class<?> superclass = subclass.getSuperclass();
    Field field;

    while (superclass != null)
    {
      field = getField(superclass, fieldName);

      if (field != null)
        return true;

      superclass = superclass.getSuperclass();
    }

    return false;
  }

  private Field getField(Class<?> theClass, String fieldName)
  {
    try
    {
      return theClass.getDeclaredField(fieldName);
    }
    catch(Exception e)
    {
      return null;
    }
  }
}