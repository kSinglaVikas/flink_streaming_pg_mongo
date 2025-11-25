package com.flink.streaming;

import com.flink.streaming.model.Order;
import com.flink.streaming.model.User;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Enriches users with their orders by maintaining state
 */
public class UserOrderEnrichmentFunction extends KeyedCoProcessFunction<Integer, User, Order, User> {
    
    private transient MapState<Integer, Order> ordersState;
    private transient MapState<Integer, User> usersState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // State to store orders by order_id
        MapStateDescriptor<Integer, Order> ordersDescriptor = new MapStateDescriptor<>(
                "orders-state",
                TypeInformation.of(Integer.class),
                TypeInformation.of(Order.class)
        );
        ordersState = getRuntimeContext().getMapState(ordersDescriptor);
        
        // State to store user
        MapStateDescriptor<Integer, User> usersDescriptor = new MapStateDescriptor<>(
                "users-state",
                TypeInformation.of(Integer.class),
                TypeInformation.of(User.class)
        );
        usersState = getRuntimeContext().getMapState(usersDescriptor);
    }

    @Override
    public void processElement1(User user, Context ctx, Collector<User> out) throws Exception {
        // Process user update
        // Get all existing orders for this user
        user.getOrders().clear();
        for (Order order : ordersState.values()) {
            if (order.getUserId() != null && order.getUserId().equals(user.getId())) {
                user.addOrder(order);
            }
        }
        
        // Store the user
        usersState.put(user.getId(), user);
        
        // Emit enriched user
        out.collect(user);
    }

    @Override
    public void processElement2(Order order, Context ctx, Collector<User> out) throws Exception {
        // Process order update
        // Store the order
        ordersState.put(order.getId(), order);
        
        // Get the user and update their orders
        User user = usersState.get(order.getUserId());
        if (user != null) {
            // Rebuild orders list
            user.getOrders().clear();
            for (Order o : ordersState.values()) {
                if (o.getUserId() != null && o.getUserId().equals(user.getId())) {
                    user.addOrder(o);
                }
            }
            
            // Emit updated user with embedded orders
            out.collect(user);
        }
    }
}
