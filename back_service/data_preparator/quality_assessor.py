class QATest:
    def __init__(self, name, description, request, conditions, table):
        self.__name = name
        self.__desc = description
        self.__request = request
        self.__conditions = conditions
        self.__table = table
        pass

    def run(self):
        result = self.__request(self.__table)
        color = self.__conditions(result)

        return {"name":self.__name,
               "description": self.__desc.format(result),
               "color":color}
        

class QualityAssessor:

    def __init__(self):
        self.__tests = list()

    def add_test(self, test):
        self.__tests.append(test)
    
    def run_tests(self, logger):
        results = list()
        for test in self.__tests:
            try:
                result = test.run()
                results.append(result)
            except:
                pass
        if len(self.__tests)>len(results):
            logger("Количество результатов тестов не совпадает с количеством тестов:  {0} против {1}".format(len(results), len(self.__tests)))
        return results
    
    def clear_tests(self):
        self.__tests = list()     