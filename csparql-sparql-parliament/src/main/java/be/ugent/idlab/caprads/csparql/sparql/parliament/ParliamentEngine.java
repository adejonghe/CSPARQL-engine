package be.ugent.idlab.caprads.csparql.sparql.parliament;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbn.parliament.jena.graph.KbGraph;
import com.bbn.parliament.jena.graph.KbGraphFactory;
import com.bbn.parliament.jena.graph.KbGraphStore;
import com.bbn.parliament.jena.graph.index.IndexFactoryRegistry;
import com.bbn.parliament.jena.graph.index.IndexManager;
import com.bbn.parliament.jena.graph.index.spatial.Constants;
import com.bbn.parliament.jena.graph.index.spatial.SpatialIndex;
import com.bbn.parliament.jena.graph.index.spatial.SpatialIndexFactory;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import eu.larkc.csparql.sparql.jena.JenaEngine;

public class ParliamentEngine extends JenaEngine {
	
	private Logger logger = LoggerFactory.getLogger(ParliamentEngine.class.getName());
	
	public ParliamentEngine() {
		super();
	}

	public String getEngineType(){
		return "parliament";
	}
	
	@Override
	public void clean() {
		model.removeAll();
		timestamps.clear();
	}
	
	@Override
	public void initialize() {
		
		logger.info("Initializing Parliament Engine");
		
		KbGraph baseGraph = KbGraphFactory.createDefaultGraph();
		model = ModelFactory.createModelForGraph(baseGraph);		
		KbGraphStore graphStore = new KbGraphStore(baseGraph);
		graphStore.initialize();		
		
        Properties properties = new Properties();
        properties.setProperty(Constants.GEOMETRY_INDEX_TYPE, Constants.GEOMETRY_INDEX_RTREE);
        properties.setProperty(Constants.GEOSPARQL_ENABLED, Boolean.TRUE.toString());
        
        SpatialIndexFactory spatialIndexFactory = new SpatialIndexFactory();
        spatialIndexFactory.configure(properties);        
        IndexFactoryRegistry.getInstance().register(spatialIndexFactory);
        
        SpatialIndex spatialIndex = spatialIndexFactory.createIndex(baseGraph, null);
        IndexManager.getInstance().register(baseGraph, null, spatialIndexFactory, spatialIndex);
        
        //graphStore.setIndexingEnabled(KbGraphStore.DEFAULT_GRAPH_NODE, true);
        
	}
}
