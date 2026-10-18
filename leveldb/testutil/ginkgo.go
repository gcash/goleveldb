package testutil

import (
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func RunSuite(t ginkgo.GinkgoTestingT, name string) {
	RunDefer()

	ginkgo.SynchronizedBeforeSuite(func() []byte {
		RunDefer("setup")
		return nil
	}, func(data []byte) {})
	ginkgo.SynchronizedAfterSuite(func() {
		RunDefer("teardown")
	}, func() {})

	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, name)
}
