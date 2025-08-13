package org.apache.flink.training.examples.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class BroadcastStateDemo {

    private static final OutputTag<Order> missingProductOrders = new OutputTag<Order>("missing-product-orders") {
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 1. 商品流：添加时间戳和水印，模拟初始化数据先加载
        DataStream<Product> productStream = env.fromElements(
                        new Product("p1001", "智能手机", "电子产品", 5999.0),
                        new Product("p1002", "笔记本电脑", "电子产品", 8999.0),
                        new Product("p1003", "运动鞋", "服装", 699.0)
                )
                // 添加时间戳（模拟数据产生时间）和水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Product>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((product, timestamp) -> System.currentTimeMillis())
                );

        // 2. 订单流：添加时间戳和水印，模拟稍晚于商品流产生
        DataStream<Order> orderStream = env.fromElements(
                        new Order("o1001", "p1001", 2),
                        new Order("o1002", "p1003", 1),
                        new Order("o1003", "p1002", 1)
                ).map(new MapFunction<Order, Order>() {
                    @Override
                    public Order map(Order order) throws Exception {
                        Thread.sleep(2000);
                        return order;
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((order, timestamp) -> System.currentTimeMillis() + 1000) // 订单时间稍晚
                );

        // 3. 定义广播状态描述符
        MapStateDescriptor<String, Product> productStateDescriptor =
                new MapStateDescriptor<>("productBroadcastState", String.class, Product.class);

        // 4. 广播商品流
        BroadcastStream<Product> broadcastProductStream = productStream.broadcast(productStateDescriptor);

        // 5. 连接订单流和广播流
        BroadcastConnectedStream<Order, Product> connectedStreams = orderStream.connect(broadcastProductStream);

        // 6. 处理连接流，解决商品未找到问题
        SingleOutputStreamOperator<OrderWithProduct> resultStream = connectedStreams.process(
                new BroadcastProcessFunction<Order, Product, OrderWithProduct>() {
                    // 存储已加载的商品ID，用于判断初始化是否完成
                    private final Set<String> loadedProductIds = new HashSet<>();

                    // 处理订单流
                    @Override
                    public void processElement(Order order,
                                               ReadOnlyContext ctx,
                                               Collector<OrderWithProduct> out) throws Exception {
                        ReadOnlyBroadcastState<String, Product> broadcastState =
                                ctx.getBroadcastState(productStateDescriptor);

                        Product product = broadcastState.get(order.getProductId());

                        if (product != null) {
                            out.collect(new OrderWithProduct(
                                    order.getOrderId(),
                                    order.getProductId(),
                                    product.getProductName(),
                                    product.getCategory(),
                                    product.getPrice(),
                                    order.getAmount()
                            ));
                        } else {
                            // 商品未找到，发送到侧输出流（可后续重试）
                            ctx.output(missingProductOrders, order);
                        }
                    }

                    // 处理商品流，更新广播状态
                    @Override
                    public void processBroadcastElement(Product product,
                                                        Context ctx,
                                                        Collector<OrderWithProduct> out) throws Exception {
                        BroadcastState<String, Product> broadcastState =
                                ctx.getBroadcastState(productStateDescriptor);

                        // 更新广播状态
                        broadcastState.put(product.getProductId(), product);
                        loadedProductIds.add(product.getProductId());
                        System.out.println("已加载商品: " + product.getProductId() +
                                "，当前已加载总数: " + loadedProductIds.size());
                    }
                }
        );

        // 7. 输出正常结果和异常订单
        resultStream.print("关联成功");
        resultStream.getSideOutput(missingProductOrders).print("商品未找到");

        env.execute("Fixed Order and Product Broadcast Join");
    }
}


class Order {
    private String orderId;
    private String productId;
    private double amount;

    // 构造函数、getter和setter
    public Order(String orderId, String productId, double amount) {
        this.orderId = orderId;
        this.productId = productId;
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getProductId() {
        return productId;
    }

    public double getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", amount=" + amount +
                '}';
    }
}

class Product {
    private String productId;
    private String productName;
    private String category;
    private double price;

    // 构造函数、getter和setter
    public Product(String productId, String productName, String category, double price) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.price = price;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public String getCategory() {
        return category;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Product{" +
                "productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", category='" + category + '\'' +
                ", price=" + price +
                '}';
    }
}

class OrderWithProduct {
    private String orderId;
    private String productId;
    private String productName;
    private String category;
    private double price;
    private double amount;

    public OrderWithProduct(String orderId, String productId, String productName,
                            String category, double price, double amount) {
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.price = price;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "OrderWithProduct{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", category='" + category + '\'' +
                ", price=" + price +
                ", amount=" + amount +
                '}';
    }
}