package be.ugent.idlab.caprads.csparql.sparql.parliament;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.data_source.Datasource;
import eu.larkc.csparql.common.hardware_resource.Memory;
import eu.larkc.csparql.common.utils.ReasonerChainingType;
import eu.larkc.csparql.sparql.api.SparqlEngine;
import eu.larkc.csparql.sparql.api.SparqlQuery;

public class ParliamentEngineSA implements SparqlEngine {
	
	private Datasource jds = new JenaDatasource();

	private Model kbModel = null;	
	
	Map<String, Model> graphs = new HashMap<String, Model>();

	private Logger logger = LoggerFactory.getLogger(ParliamentEngineSA.class.getName());

	public String getEngineType(){
		return "parliamentSA";
	}

	public void addStatement(final String subject, final String predicate, final String object) {
		addStatement(subject, predicate, object, 0);
	}

	public void addStatement(final String subject, final String predicate, final String object, final long timestamp) {
				
		final Statement s;

		String[] objectParts = object.split("\\^\\^");
		
		if (objectParts.length > 1) {

			TypeMapper tm = TypeMapper.getInstance();
			RDFDatatype d = tm.getTypeByName(objectParts[1]);
			Literal lObject = kbModel.createTypedLiteral(objectParts[0].replaceAll("\"", ""),d);

			s = new StatementImpl(new ResourceImpl(subject), new PropertyImpl(predicate), lObject); 

		} else {

			s = new StatementImpl(new ResourceImpl(subject), new PropertyImpl(predicate), new ResourceImpl(object));    	 
		}

		System.out.println(s);
		kbModel.add(s);
	}

	public void clean() {
		//kbModel.remove(kbModel);
		kbModel.removeAll();
	}


	public void destroy() {
		kbModel.close();
	}


	public RDFTable evaluateQuery(final SparqlQuery query) {
		
		long startTS = System.currentTimeMillis();		
		
		Query q = QueryFactory.create(query.getQueryCommand(), Syntax.syntaxSPARQL_11);		
		
		for(String s: q.getGraphURIs()){
			List<RDFTuple> list = jds.getNamedModel(s);
			for(RDFTuple t : list)
				addStatement(t.get(0), t.get(1), t.get(2));
		}
		
		QueryExecution qexec = QueryExecutionFactory.create(q, kbModel);				
		
		RDFTable table = new RDFTable();
		
		if (q.isSelectType())
		{

			final ResultSet resultSet = qexec.execSelect();

			table = new RDFTable(resultSet.getResultVars());

			ByteArrayOutputStream bos = new ByteArrayOutputStream();

			ResultSetRewindable tempResultSet = ResultSetFactory.makeRewindable(resultSet);

			ResultSetFormatter.outputAsJSON(bos, tempResultSet);
			table.setJsonSerialization(bos.toString());

			tempResultSet.reset();

			for (; tempResultSet.hasNext();) {
				final RDFTuple tuple = new RDFTuple();
				QuerySolution soln = tempResultSet.nextSolution();

				for (String s : table.getNames()) {
					RDFNode n = soln.get(s);
					if (n == null)
						tuple.addFields("");
					else
						tuple.addFields(format(n));
				}
				table.add(tuple);
			}
		}
		else if (q.isAskType())
		{
			table = new RDFTable("Answer");
			final RDFTuple tuple = new RDFTuple();
			tuple.addFields("" + qexec.execAsk());
			table.add(tuple);
		}
		else if (q.isDescribeType() || q.isConstructType())
		{
			Model m = null;
			if (q.isDescribeType())
				m = qexec.execDescribe();
			else
				m = qexec.execConstruct();

			table = new RDFTable("Subject", "Predicate", "Object");

			StringWriter w = new StringWriter();
			m.write(w,"RDF/JSON");
			table.setJsonSerialization(w.toString());

			StmtIterator it = m.listStatements();
			while (it.hasNext())
			{
				final RDFTuple tuple = new RDFTuple();
				Statement stm = it.next();
				tuple.addFields(formatSubject(stm.getSubject()),formatPredicate(stm.getPredicate()), format(stm.getObject())); 
				table.add(tuple);
			}
		}
		
		long endTS = System.currentTimeMillis();

		Object[] object = new Object[6];

		object[0] = query.getId();
		object[1] = (endTS - startTS);
		object[2] = table.size();
		object[3] = Memory.getTotalMemory();
		object[4] = Memory.getFreeMemory();
		object[5] = Memory.getMemoryUsage();

		logger.debug("Information about execution of query {} \n Execution Time : {} \n Results Number : {} \n Total Memory : {} mb \n " +
				"Free Memory : {} mb \n Memory Usage : {} mb", object);
		
		return table;
	}

	private String format(RDFNode n) {
		if (n.isLiteral())
			return "\"" + n.asLiteral().getLexicalForm() + "\"^^" + n.asLiteral().getDatatypeURI(); 
		else
			return n.toString();
	}

	private String formatPredicate(Property predicate) {
		return predicate.toString();
	}

	private String formatSubject(Resource subject) {
		return subject.toString();
	}

	private List<RDFTuple> modelToTupleList(Model m){
		List<RDFTuple> list = new ArrayList<RDFTuple>();
		StmtIterator it = m.listStatements();
		while (it.hasNext())
		{
			final RDFTuple tuple = new RDFTuple();
			Statement stm = it.next();
			tuple.addFields(formatSubject(stm.getSubject()),formatPredicate(stm.getPredicate()), format(stm.getObject())); 
			list.add(tuple);
		}
		return list;
	}
	
	@Override
	public void initialize() {
		
		logger.info("Initializing SA Parliament Engine");
		
		KbGraph baseGraph = KbGraphFactory.createDefaultGraph();
		kbModel = ModelFactory.createModelForGraph(baseGraph);		
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

	@Override
	public RDFTable evaluateGeneralQueryOverDatasource(String queryBody){
		return jds.evaluateGeneralQuery(queryBody);
	}

	@Override
	public void execUpdateQueryOverDatasource(String queryBody){
		jds.execUpdateQuery(queryBody);
	}

	@Override
	public void parseSparqlQuery(SparqlQuery query) throws ParseException {
		Query spQuery = QueryFactory.create(query.getQueryCommand(), Syntax.syntaxSPARQL_11);
		for (String s : spQuery.getGraphURIs()) {
			if (!jds.containsNamedModel(s))
				throw new ParseException("The model in the FROM clause is missing in the internal dataset, please put the static model in the dataset using putStaticNamedModel(String iri, String location) method of the engine.", 0);
		}
	}

	@Override
	public void putStaticNamedModel(String iri, String modelReference) {
		Model m = ModelFactory.createDefaultModel();
		try {
			m.read(iri);
		} catch (Exception e) {
			StringReader sr = new StringReader(modelReference);
			try {
				m.read(sr, null, "RDF/XML");
			} catch (Exception e1) {
				try {
					sr = new StringReader(modelReference);
					m.read(sr, null, "TURTLE");
				} catch (Exception e2) {
					try {
						sr = new StringReader(modelReference);
						m.read(sr, null, "N-TRIPLE");
					} catch (Exception e3) {
						sr = new StringReader(modelReference);
						m.read(sr, null, "RDF/JSON");
					}
				}
			}
			sr.close();
		}

		jds.putNamedModel(iri, modelToTupleList(m));
	}

	@Override
	public void removeStaticNamedModel(String iri) {
		jds.removeNamedModel(iri);
	}

	@Override
	public Datasource getDataSource() {
		return jds;
	}

	@Override
	public boolean getInferenceStatus() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setReasonerMap(Object reasonerMap) {
		// TODO Auto-generated method stub	
	}

	@Override
	public void arrestInference(String queryId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void addReasonerToReasonerMap(String queryId, Object reasoner) {
		// TODO Auto-generated method stub
	}

	@Override
	public void restartInference(String queryId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void updateReasoner(String queryId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void updateReasoner(String queryId, String rulesFile, ReasonerChainingType chainingType) {
		// TODO Auto-generated method stub
	}

	@Override
	public void updateReasoner(String queryId, String rulesFile, ReasonerChainingType chainingType, String tBoxFile) {
		// TODO Auto-generated method stub
	}
}
