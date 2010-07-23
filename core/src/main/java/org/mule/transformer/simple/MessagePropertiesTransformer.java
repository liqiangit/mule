/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transformer.simple;

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleContext;
import org.mule.api.MuleMessage;
import org.mule.api.context.MuleContextAware;
import org.mule.api.transformer.TransformerException;
import org.mule.api.transport.PropertyScope;
import org.mule.config.i18n.CoreMessages;
import org.mule.transformer.AbstractMessageAwareTransformer;
import org.mule.transformer.types.DataTypeFactory;
import org.mule.transport.NullPayload;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A configurable message transformer that allows users to add, overwrite and delete
 * properties on the current message. Users can set a {@link List} of
 * 'deleteProperties' names to remove from the message and can also set a {@link Map}
 * of 'addProperties' that will be added to the message and possibly overwrite
 * existing properties with the same name. <p/> If {@link #overwrite} is set to
 * <code>false</code>, and a property exists on the message (even if the value is
 * <code>null</code>, it will be left intact. The transformer then acts as a more
 * gentle 'enricher'. The default setting is <code>true</code>.
 */
public class MessagePropertiesTransformer extends AbstractMessageAwareTransformer implements MuleContextAware
{
    private List<String> deleteProperties = null;
    private Map<String, Object> addProperties = null;
    /** the properties map containing rename mappings for message properties */
    private Map<String, String> renameProperties;
    private String getProperty;
    private boolean overwrite = true;
    // outbound it the default scope
    private PropertyScope scope = PropertyScope.OUTBOUND;

    private MuleContext muleContext;

    public MessagePropertiesTransformer()
    {
        registerSourceType(Object.class);
        setReturnDataType(DataTypeFactory.create(Object.class));
    }

    @Override
    public Object clone() throws CloneNotSupportedException
    {
        MessagePropertiesTransformer clone = (MessagePropertiesTransformer)super.clone();

        if (deleteProperties != null)
        {
            clone.setDeleteProperties(new ArrayList<String>(deleteProperties));
        }

        if (addProperties != null)
        {
            clone.setAddProperties(new HashMap<String, Object>(addProperties));
        }

        if (renameProperties != null)
        {
            clone.setRenameProperties(new HashMap<String, String>(renameProperties));
        }
        return clone;
    }

    @Override
    public void setMuleContext(MuleContext context)
    {
        this.muleContext = context;
    }

    @Override
    public Object transform(MuleMessage message, String outputEncoding) throws TransformerException
    {
        if (deleteProperties != null && deleteProperties.size() > 0)
        {
            for (Iterator iterator = deleteProperties.iterator(); iterator.hasNext();)
            {
                message.removeProperty(iterator.next().toString(), scope);
            }
        }

        if (addProperties != null && addProperties.size() > 0)
        {
            for (Iterator iterator = addProperties.entrySet().iterator(); iterator.hasNext();)
            {
                Map.Entry entry = (Map.Entry)iterator.next();
                if (entry.getKey() == null)
                {
                    logger.error("Setting Null property keys is not supported, this entry is being ignored");
                }
                else
                {
                    final String key = entry.getKey().toString();

                    Object value= entry.getValue();
                    Object realValue = value;

                    //Enable expression support for property values
                    if (muleContext.getExpressionManager().isValidExpression(value.toString()))
                    {
                        realValue = muleContext.getExpressionManager().evaluate(value.toString(), message);
                    }

                    if (realValue!=null)
                    {
                        if (message.getProperty(key, scope) != null)
                        {
                            if (overwrite)
                            {
                                logger.debug("Overwriting message property " + key);
                                message.setProperty(key, realValue, scope);
                            }
                            else if(logger.isDebugEnabled())
                            {
                                logger.debug(MessageFormat.format(
                                    "Message already contains the property and overwrite is false, skipping: key={0}, value={1}, scope={2}",
                                    key, realValue, scope));
                            }
                        }
                        //If value is null an exception will not be thrown if the key was marked as optional (with a '?'). If not
                        //optional the expression evaluator will throw an exception
                        else
                        {
                            message.setProperty(key, realValue, scope);
                        }
                    }
                    else if(logger.isInfoEnabled())
                        {
                            logger.info(MessageFormat.format(
                                    "Property with key '{0}', not found on message using '{1}'. Since the value was marked optional, nothing was set on the message for this property",
                                    key, value));
                        }
                }
            }
        }

        /* perform renaming transformation */
        if (this.renameProperties != null && this.renameProperties.size() > 0)
        {
            for (Map.Entry<String, String> entry : this.renameProperties.entrySet())
            {
                if (entry.getKey() == null)
                {
                    logger.error("Setting Null property keys is not supported, this entry is being ignored");
                }
                else
                {
                    final String key = entry.getKey();
                    String value = entry.getValue();

                    if (value == null)
                    {
                        logger.error("Setting Null property values for renameProperties is not supported, this entry is being ignored");
                    }
                    else
                    {
                        //Enable expression support for property values
                        if(muleContext.getExpressionManager().isValidExpression(value))
                        {
                            Object temp = muleContext.getExpressionManager().evaluate(value, message);
                            if (temp!=null)
                            {
                                value = temp.toString();
                            }
                        }

                        /* log transformation */
                        if (logger.isDebugEnabled() && message.getProperty(key, scope) == null)
                        {
                            logger.debug("renaming message property " + key + " to " + value);
                        }

                        /*
                         * store current value of the property. then remove key and
                         * store value under new key
                         */
                        if (scope == null)
                        {
                            //If scope is not specified, rename the property in the same scope
                            //Cannot rename in Inbound Scope
                            //TODO Do Session and Application Scope still exist

                            if (message.getInvocationPropertyNames().contains(key))
                            {
                                renameInScope(key, value, PropertyScope.INVOCATION, message);
                            }
                            else if(message.getOutboundPropertyNames().contains(key))
                            {
                                renameInScope(key, value, PropertyScope.OUTBOUND, message);
                            }
                            else if(message.getInboundPropertyNames().contains(key))
                            {
                                throw new TransformerException(CoreMessages.cannotRenameInboundScopeProperty(key, value));
                            }
                        }
                        else
                        {
                            renameInScope(key, value, scope, message);
                        }
                    }
                }
            }
        }
        
        if (getProperty != null)
        {
            Object prop = message.getProperty(getProperty, scope);
            if (prop != null)
            {
                message = new DefaultMuleMessage(prop, muleContext);
            }
            else
            {
                message = new DefaultMuleMessage(NullPayload.getInstance(), muleContext);
            }
        }

        return message;
    }

    protected void renameInScope(String oldKey, String newKey, PropertyScope scope, MuleMessage message)
    {
        Object propValue = message.getProperty(oldKey, scope);
        message.removeProperty(oldKey, scope);
        message.setProperty(newKey, propValue, scope);
    }

    public List getDeleteProperties()
    {
        return deleteProperties;
    }

    public void setDeleteProperties(List<String> deleteProperties)
    {
        this.deleteProperties = deleteProperties;
    }

    public Map getAddProperties()
    {
        return addProperties;
    }

    public void setAddProperties(Map<String, Object> addProperties)
    {
        this.addProperties = addProperties;
    }

    /**
     * @return the renameProperties
     */
    public Map getRenameProperties()
    {
        return this.renameProperties;
    }

    /**
     * @param renameProperties the renameProperties to set
     */
    public void setRenameProperties(Map<String, String> renameProperties)
    {
        this.renameProperties = renameProperties;
    }

    public String getGetProperty()
    {
        return getProperty;
    }

    public void setGetProperty(String getProperty)
    {
        this.getProperty = getProperty;
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    public void setOverwrite(final boolean overwrite)
    {
        this.overwrite = overwrite;
    }

    public PropertyScope getScope()
    {
        return scope;
    }

    public void setScope(PropertyScope scope)
    {
        this.scope = scope;
    }
    
    /**
     * For XML-based config
     *
     * @return The string value name for a {@link org.mule.api.transport.PropertyScope}
     */
    public String getScopeName()
    {
        return scope != null ? scope.getScopeName() : null;
    }

    /**
     * For XML-based config
     * @param scopeName The string value name for a {@link org.mule.api.transport.PropertyScope}
     */
    public void setScopeName(String scopeName)
    {
        this.scope = PropertyScope.get(scopeName);
    }
}
