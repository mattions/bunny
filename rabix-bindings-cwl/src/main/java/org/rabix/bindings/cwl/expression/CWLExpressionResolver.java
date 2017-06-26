package org.rabix.bindings.cwl.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.rabix.bindings.cwl.bean.CWLJob;
import org.rabix.bindings.cwl.bean.CWLRuntime;
import org.rabix.bindings.cwl.bean.resource.requirement.CWLInlineJavascriptRequirement;
import org.rabix.bindings.cwl.expression.javascript.CWLExpressionJavascriptResolver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class CWLExpressionResolver {

  public static String KEY_EXPRESSION_VALUE = "script";
  public static String KEY_EXPRESSION_LANGUAGE = "engine";

  private static String segSymbol = "\\w+";
  private static String segSingle = "\\['([^']|\\\\')+'\\]";
  private static String segDouble = "\\[\"([^\"]|\\\\\")+\"\\]";
  private static String segIndex = "\\[[0-9]+\\]";

  private static String segments = String.format("(.%s|%s|%s|%s)", segSymbol, segSingle, segDouble, segIndex);

  private static String paramRe = String.format("\\$\\((%s)%s*\\)", segSymbol, segments);

  private static Pattern segPattern = Pattern.compile(segments);
  private static Pattern pattern = Pattern.compile(paramRe);

  public static final ObjectMapper sortMapper = new ObjectMapper();

  static {
    sortMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  @SuppressWarnings({ "unchecked" })
  public static <T> T resolve(final Object expression, final CWLJob job, final Object self) throws CWLExpressionException {
    if (expression == null) {
      return null;
    }
    if (isExpressionObject(expression)) {
      String script = (String) ((Map<?, ?>) expression).get(KEY_EXPRESSION_VALUE);
      List<String> expressionLibs = Collections.<String>emptyList();
      CWLInlineJavascriptRequirement inlineJavascriptRequirement = job.getApp().getInlineJavascriptRequirement();
      if (inlineJavascriptRequirement != null) {
        expressionLibs = inlineJavascriptRequirement.getExpressionLib();
      }
      return (T) CWLExpressionJavascriptResolver.evaluate(job.getInputs(), self, script, job.getRuntime(), expressionLibs);
    }
    if (expression instanceof String) {
      if (job.isInlineJavascriptEnabled()) {
        List<String> expressionLibs = Collections.<String>emptyList();
        CWLInlineJavascriptRequirement inlineJavascriptRequirement = job.getApp().getInlineJavascriptRequirement();
        if (inlineJavascriptRequirement != null) {
          expressionLibs = inlineJavascriptRequirement.getExpressionLib();
        }
        return (T) javascriptInterpolate(job, self, (String) expression, job.getRuntime(), expressionLibs);
      } else {
        Map<String, Object> vars = new HashMap<>();
        vars.put("inputs", job.getInputs());
        vars.put("self", self);

        CWLRuntime runtime = job.getRuntime();
        if (runtime != null) {
          vars.put("runtime", runtime.toMap());
        }
        return (T) paramInterpolate((String) expression, vars, true);
      }
    }
    return (T) expression;
  }

  public static boolean isExpressionObject(Object expression) {
    return expression instanceof Map<?,?>  && ((Map<?,?>) expression).containsKey(KEY_EXPRESSION_VALUE)  && ((Map<?,?>) expression).containsKey(KEY_EXPRESSION_LANGUAGE);
  }

  private static Object nextSegment(String remaining, Object vars) throws CWLExpressionException {
    if (vars == null) {
      return null;
    }
    if (!StringUtils.isEmpty(remaining)) {
      Matcher m = segPattern.matcher(remaining);
      if (m.find()) {
        if (m.group(0).startsWith(".")) {
          return nextSegment(remaining.substring(m.end(0)), ((Map<?, ?>) vars).get(m.group(0).substring(1)));
        } else if (m.group(0).charAt(1) == '\"' || m.group(0).charAt(1) == '\'') {
          Character start = m.group(0).charAt(1);
          String key = m.group(0).substring(2, m.group(0).lastIndexOf(start));
          key = key.replace("\\'", "'");
          key = key.replace("\\\"", "\"");
          return nextSegment(remaining.substring(m.end(0)), ((Map<?, ?>) vars).get(key));
        } else {
          String key = m.group(0).substring(1, m.group(0).length());
          Integer keyInt = Integer.parseInt(key);

          Object remainingVars = null;
          if (vars instanceof List<?>) {
            if (((List<?>) vars).size() <= keyInt) {
              throw new CWLExpressionException("Could not get value from " + vars + " at position " + keyInt);
            }
            remainingVars = ((List<?>) vars).get(keyInt);
          } else if (vars instanceof Map<?,?>) {
            remainingVars = ((Map<?,?>) vars).get(keyInt);
          }
          return nextSegment(remaining.substring(m.end(0)), remainingVars);
        }
      }
    }
    return vars;
  }

  private static Object paramInterpolate(String ex, Map<String, Object> obj, boolean strip) throws CWLExpressionException {
    Matcher m = pattern.matcher(ex);
    if (m.find()) {
      Object leaf = nextSegment(m.group(0).substring(m.end(1) - m.start(0), m.group(0).length() - 1), obj.get(m.group(1)));
      if (strip && ex.trim().length() == m.group(0).length()) {
        return leaf;
      } else {
        try {
          String leafStr = sortMapper.writeValueAsString(leaf);
          if (leafStr.startsWith("\"")) {
            leafStr = leafStr.substring(1, leafStr.length() - 1);
          }
          return ex.substring(0, m.start(0)) + leafStr + paramInterpolate(ex.substring(m.end(0)), obj, false);
        } catch (JsonProcessingException e) {
          throw new CWLExpressionException("Failed to serialize " + leaf + " to JSON.", e);
        }
      }
    }
    return ex;
  }

  private static Object javascriptInterpolate(CWLJob job, Object self, String expression, CWLRuntime runtime, List<String> engineConfigs) throws CWLExpressionException {
    expression = expression.trim();

    List<Object> parts = new ArrayList<>();

    int[] scanned = scanJavascriptExpression(expression);

    while (scanned != null) {
      parts.add(expression.substring(0, scanned[0]));

      Map<String, Object> inputs = null;
      if(job != null) {
        inputs = job.getInputs();
      }
      Object evaluated = CWLExpressionJavascriptResolver.evaluate(inputs, self, expression.substring(scanned[0] + 1, scanned[1]), runtime, engineConfigs);
      if (scanned[0] == 0 && scanned[1] == expression.length()) {
        return evaluated;
      }
      String leafStr = null;
      try {
        leafStr = sortMapper.writeValueAsString(evaluated);
      } catch (JsonProcessingException e) {
        throw new CWLExpressionException("Failed to serialize " + evaluated + " to JSON.", e);
      }
      if (leafStr.startsWith("\"")) {
        leafStr = leafStr.substring(1, leafStr.length() - 1);
      }
      parts.add(leafStr);

      expression = expression.substring(scanned[1]);
      scanned = scanJavascriptExpression(expression);
    }
    parts.add(expression);
    return StringUtils.join(parts, "");
  }

  private static int[] scanJavascriptExpression(String expression) throws CWLExpressionException {
    int DEFAULT = 0;
    int DOLLAR = 1;
    int PAREN = 2;
    int BRACE = 3;
    int SINGLE_QUOTE = 4;
    int DOUBLE_QUOTE = 5;
    int BACKSLASH = 6;

    int i = 0;
    Stack<Integer> stack = new Stack<>();
    stack.push(DEFAULT);

    int start = 0;
    while (i < expression.length()) {
      int state = stack.peek();
      Character c = expression.charAt(i);

      if (state == DEFAULT) {
        if (c == '$') {
          stack.push(DOLLAR);
        } else if (c == '\\') {
          stack.push(BACKSLASH);
        }
      } else if (state == BACKSLASH) {
        stack.pop();
      } else if (state == DOLLAR) {
        if (c == '(') {
          start = i - 1;
          stack.push(PAREN);
        } else if (c == '{') {
          start = i - 1;
          stack.push(BRACE);
        }
      } else if (state == PAREN) {
        if (c == '(') {
          stack.push(PAREN);
        } else if (c == ')') {
          stack.pop();
          if (stack.peek() == DOLLAR) {
            return new int[] { start, i + 1 };
          }
        } else if (c == '\'') {
          stack.push(SINGLE_QUOTE);
        } else if (c == '"') {
          stack.push(DOUBLE_QUOTE);
        }
      } else if (state == BRACE) {
        if (c == '{') {
          stack.push(BRACE);
        } else if (c == '}') {
          stack.pop();
          if (stack.peek() == DOLLAR) {
            return new int[] { start, i + 1 };
          }
        } else if (c == '\'') {
          stack.push(SINGLE_QUOTE);
        } else if (c == '"') {
          stack.push(DOUBLE_QUOTE);
        }
      } else if (state == SINGLE_QUOTE) {
        if (c == '\'') {
          stack.pop();
        } else if (c == '\\') {
          stack.push(BACKSLASH);
        }
      } else if (state == DOUBLE_QUOTE) {
        if (c == '\"') {
          stack.pop();
        } else if (c == '\\') {
          stack.push(BACKSLASH);
        }
      }
      i++;
    }
    if (stack.size() > 1) {
      throw new CWLExpressionException("Substitution error, unfinished block starting at position " + start + " : " + expression.substring(start));
    }
    return null;
  }


}