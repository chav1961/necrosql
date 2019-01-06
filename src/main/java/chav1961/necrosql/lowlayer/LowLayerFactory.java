package chav1961.necrosql.lowlayer;

import java.util.ServiceLoader;

import chav1961.necrosql.interfaces.LowLayerInterface;

public class LowLayerFactory {
	public static LowLayerInterface getLowLayer() {
		for (LowLayerInterface item : ServiceLoader.load(LowLayerInterface.class,Thread.currentThread().getContextClassLoader())) {
			return item;
		}
		throw new UnsupportedOperationException("No services implementing "+LowLayerInterface.class.getName()+" was detected by SPI!");
	}
}
